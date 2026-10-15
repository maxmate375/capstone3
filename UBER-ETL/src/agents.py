"""
agents.py - multi-agent system that runs, validates and reports on the ETL pipeline.

LangGraph StateGraph with the routing from the project spec:

    __start__ -> supervisor
    supervisor --run_etl--> etl | --quality--> quality | --analytics--> analytics | --explain--> explain
    etl --success--> quality         etl --failure--> __end__
    quality --pass--> analytics      quality --fail--> __end__
    analytics -> __end__             explain -> __end__

Foundry GPT (one client, JSON outputs) is used for routing, severity classification, failure
explanations and answering questions. Deterministic rules decide: a failed hard rule always ends
the run and is always "critical"; the model cannot override it.
"""

import json
import operator
import re
import time
from datetime import date, datetime, timezone
from typing import Annotated, TypedDict

from src import config


# ===========================================================================
# Foundry GPT client
# ===========================================================================
class FoundryClient:
    """Single GPT client for all agents. Entra ID auth (service principal) or API key from the secret scope."""

    def __init__(self, spark):
        from openai import AzureOpenAI
        endpoint = config.get_setting(spark, "FOUNDRY_ENDPOINT", "foundry_endpoint")
        self.deployment = config.get_setting(spark, "FOUNDRY_DEPLOYMENT", "foundry_deployment")
        if config.FOUNDRY_AUTH_MODE == "key":
            self.client = AzureOpenAI(azure_endpoint=endpoint, api_version=config.FOUNDRY_API_VERSION,
                                      api_key=config.get_secret(spark, "foundry_api_key"))
        else:
            from azure.identity import ClientSecretCredential, get_bearer_token_provider
            credential = ClientSecretCredential(config.get_secret(spark, "tenant_id"),
                                                config.get_secret(spark, "client_id"),
                                                config.get_secret(spark, "client_secret"))
            self.client = AzureOpenAI(
                azure_endpoint=endpoint, api_version=config.FOUNDRY_API_VERSION,
                azure_ad_token_provider=get_bearer_token_provider(credential, "https://cognitiveservices.azure.com/.default"))
        self.tokens_used = 0

    def ask_json(self, instructions: str, data: dict) -> dict:
        """Send masked, structured data; return the model's JSON answer ({} once the token cap is reached)."""
        if self.tokens_used >= config.LLM_MAX_TOKENS_PER_RUN:
            return {}
        response = self.client.chat.completions.create(
            model=self.deployment,
            messages=[{"role": "system", "content": instructions + " Reply with one JSON object only."},
                      {"role": "user", "content": mask(json.dumps(data, default=str))}],
            temperature=config.LLM_TEMPERATURE,
            max_tokens=config.LLM_MAX_TOKENS_PER_CALL,
            response_format={"type": "json_object"},
        )
        self.tokens_used += response.usage.total_tokens
        return json.loads(response.choices[0].message.content)


def mask(text: str) -> str:
    """Guardrail: mask Booking and Customer IDs before anything is sent to the model."""
    return re.sub(r"\b(CNR|CID)\d+", lambda m: m.group(1) + "*****", text)


# ===========================================================================
# Tools: the only actions agents can take
# ===========================================================================
class Tools:
    def __init__(self, spark, approvals: dict):
        from databricks.sdk import WorkspaceClient
        self.spark = spark
        self.approvals = approvals          # human approvals for destructive actions
        self.ws = WorkspaceClient()
        self.calls = []                     # every tool call is logged here

    def use(self, agent: str, tool: str, **args):
        """Call an approved tool and log it."""
        approved = {"run_etl_job", "get_run_status", "run_quality_checks",
                    "get_table_stats", "compare_row_counts", "write_report"}
        if tool not in approved:
            raise PermissionError(f"Tool {tool!r} is not approved")
        start = time.time()
        log = {"agent": agent, "tool": tool, "args": {k: v for k, v in args.items() if k != "report"}}
        try:
            out = getattr(self, tool)(**args)
            log["status"] = "ok"
            return out
        except Exception as e:
            log["status"] = f"error: {e}"[:300]
            raise
        finally:
            log["seconds"] = round(time.time() - start, 1)
            self.calls.append(log)
            print(f"[tool] {agent}.{tool} {log['args']} -> {log['status']} ({log['seconds']}s)")

    def run_etl_job(self, mode: str, start_date: str = "", end_date: str = "") -> int:
        """Trigger the Databricks Job's Bronze and Silver tasks. Returns the run ID."""
        if mode == "full" and not self.approvals.get("overwrite"):
            raise PermissionError("A full load overwrites Bronze and Silver: human approval required")
        if mode == "incremental":
            days = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1
            if days > config.MAX_BACKFILL_DAYS and not self.approvals.get("backfill"):
                raise PermissionError(f"Backfill of {days} days is over the {config.MAX_BACKFILL_DAYS}-day limit: "
                                      "human approval required")
        job = next(iter(self.ws.jobs.list(name=config.JOB_NAME)))
        run = self.ws.jobs.run_now(job_id=job.job_id, only=["bronze", "silver"],
                                   job_parameters={"mode": mode, "start_date": start_date, "end_date": end_date})
        return run.response.run_id

    def get_run_status(self, run_id: int) -> dict:
        """Poll the run until it ends (or times out) and collect each task's state and error."""
        deadline = time.time() + config.JOB_TIMEOUT_SECONDS
        while True:
            run = self.ws.jobs.get_run(run_id=run_id)
            if run.state.life_cycle_state.value in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                break
            if time.time() > deadline:
                self.ws.jobs.cancel_run(run_id=run_id)
                return {"result": "TIMEDOUT", "tasks": []}
            time.sleep(config.JOB_POLL_SECONDS)
        tasks = []
        for t in run.tasks:
            output = self.ws.jobs.get_run_output(run_id=t.run_id)
            tasks.append({"task": t.task_key, "state": t.state.result_state.value if t.state.result_state else None,
                          "error": output.error, "logs": (output.logs or output.error_trace or "")[-1500:]})
        return {"result": run.state.result_state.value if run.state.result_state else run.state.life_cycle_state.value,
                "run_page_url": run.run_page_url, "tasks": tasks}

    def run_quality_checks(self, layers: list, previous_completion_rate=None) -> dict:
        from src import quality_checks
        return quality_checks.run_quality_checks(self.spark, layers, previous_completion_rate)

    def get_table_stats(self, layer: str) -> dict:
        path = {"bronze": config.BRONZE_PATH, "silver": config.SILVER_PATH, "quarantine": config.QUARANTINE_PATH}[layer]
        from pyspark.sql import functions as F
        df = self.spark.read.format("delta").load(path)
        r = df.agg(F.count("*").alias("rows"), F.min("booking_date").alias("first"), F.max("booking_date").alias("last")).first()
        return {"layer": layer, "rows": r["rows"], "first_date": str(r["first"]), "last_date": str(r["last"])}

    def compare_row_counts(self) -> dict:
        counts = {layer: self.get_table_stats(layer)["rows"] for layer in ["bronze", "silver", "quarantine"]}
        counts["reconciled"] = counts["bronze"] == counts["silver"] + counts["quarantine"]
        return counts

    def write_report(self, run_id: str, report: dict) -> str:
        path = f"{config.REPORTS_PATH}/{run_id}.json"
        if not any(path.startswith(p) for p in config.AGENT_WRITE_PATHS):   # guardrail: reports/ only
            raise PermissionError(f"Agents may not write to {path}")
        config.get_dbutils(self.spark).fs.put(path, json.dumps(report, indent=2, default=str), overwrite=True)
        return path


# ===========================================================================
# Shared state
# ===========================================================================
class State(TypedDict, total=False):
    run_id: str
    request: str
    route: str
    mode: str
    start_date: str
    end_date: str
    nodes_run: Annotated[list, operator.add]
    findings: Annotated[list, operator.add]
    row_counts: dict
    checks_passed: Annotated[int, operator.add]
    checks_failed: Annotated[int, operator.add]
    status: str
    stopped_at: str
    escalated_to_human: bool
    answer: str


ROUTER_PROMPT = ("You route requests for an Uber ride-booking ETL pipeline (Bronze -> Silver -> Gold on Databricks). "
                 'Return {"route": "run_etl" | "quality" | "analytics" | "explain"}. '
                 "run_etl = load or reload data; quality = validate Bronze and Silver; "
                 "analytics = rebuild and validate Gold; explain = a question about runs, data or results.")
SEVERITY_PROMPT = ("You are a data-quality engineer for an Uber ride-booking pipeline. For each failed check "
                   "(structured results, no raw data) return "
                   '{"findings": [{"rule", "severity": "critical"|"warning"|"info", "explanation", '
                   '"recommended_action"}]}. Explain the likely cause citing the metric and threshold. '
                   "Meaning of the rules: metric_value is the SHARE of rows failing (0.05 = 5%) unless noted; "
                   "consistency_flag_matches_status = a cancellation/incomplete flag (1 or null) contradicts Booking Status; "
                   "consistency_reason_present_with_flag = a reason column is filled without its flag or vice versa; "
                   "uniqueness_* = share of duplicate Booking IDs; timeliness_missing_days = number of dates with no data; "
                   "volume_vs_previous_load = share by which the row count DROPPED versus the previous load; "
                   "reconciliation_bronze_silver = rows missing between Bronze and Silver + Quarantine.")
FAILURE_PROMPT = ("A Databricks Job task of the Uber ride ETL failed. From the error and log tail return "
                  '{"explanation": "<plain-language root cause>", "recommended_action": "<fix>"}.')
EXPLAIN_PROMPT = ("Answer the question about the Uber ride ETL pipeline in plain language using only the context "
                  'given (recent run reports and table stats). Return {"answer": "..."}.')


# ===========================================================================
# Agents
# ===========================================================================
class AgentSystem:
    def __init__(self, spark, approvals: dict = None):
        self.spark = spark
        self.llm = FoundryClient(spark)
        self.tools = Tools(spark, approvals or {})
        self.app = self.build_graph()

    # ---- nodes ---------------------------------------------------------------
    def supervisor(self, state: State) -> dict:
        route = self.llm.ask_json(ROUTER_PROMPT, {"request": state["request"]}).get("route")
        if route not in ("run_etl", "quality", "analytics", "explain"):
            route = "explain"           # unknown request: answer, never act
        return {"route": route, "nodes_run": ["supervisor"]}

    def etl(self, state: State) -> dict:
        mode = state.get("mode", "incremental")
        try:
            for attempt in range(1, config.AGENT_MAX_RETRIES + 2):        # retry limit
                run_id = self.tools.use("etl", "run_etl_job", mode=mode,
                                        start_date=state.get("start_date", ""), end_date=state.get("end_date", ""))
                status = self.tools.use("etl", "get_run_status", run_id=run_id)
                if status["result"] != "INTERNAL_ERROR":                     # only platform errors are retried
                    break
        except PermissionError as e:                                        # guardrail: needs a human
            return {"nodes_run": ["etl"], "status": "FAIL", "stopped_at": "etl", "escalated_to_human": True,
                    "findings": [{"rule": "human_approval_required", "severity": "critical",
                                  "explanation": str(e), "recommended_action": "Approve the action and re-run."}]}
        if status["result"] == "SUCCESS":
            return {"nodes_run": ["etl"]}
        # the task that actually failed (tasks not run by the agent show as EXCLUDED and are ignored)
        failed = next((t for t in status["tasks"] if t["state"] in ("FAILED", "TIMEDOUT", "INTERNAL_ERROR")),
                      {"task": "job", "error": status["result"]})
        ai = self.llm.ask_json(FAILURE_PROMPT, {"task": failed["task"], "error": failed.get("error"),
                                                "log_tail": failed.get("logs", "")[-800:]})
        return {"nodes_run": ["etl"], "status": "FAIL", "stopped_at": "etl", "escalated_to_human": True,
                "findings": [{"rule": f"job_task_{failed['task']}_failed", "severity": "critical",
                              "explanation": ai.get("explanation", str(failed.get("error"))),
                              "recommended_action": ai.get("recommended_action", "See the Job run page: "
                                                           + str(status.get("run_page_url")))}]}

    def classify(self, checks: list) -> list:
        """GPT classifies severity and explains; a failed hard rule is always critical."""
        failed = [c for c in checks if c["status"] == "FAIL"]
        if not failed:
            return []
        ai = {f.get("rule"): f for f in self.llm.ask_json(SEVERITY_PROMPT, {"failed_checks": failed}).get("findings", [])}
        findings = []
        for c in failed:
            f = ai.get(c["rule"], {})
            severity = "critical" if c["hard"] else (f.get("severity") if f.get("severity") in ("warning", "info") else "warning")
            findings.append({"rule": c["rule"], "severity": severity,
                             "explanation": f.get("explanation", f"{c['metric_value']} exceeds threshold {c['threshold']}"),
                             "recommended_action": f.get("recommended_action", "Investigate the failing records.")})
        return findings

    def quality(self, state: State) -> dict:
        out = self.tools.use("quality", "run_quality_checks", layers=["bronze", "silver"])
        counts = self.tools.use("quality", "compare_row_counts")
        update = {"nodes_run": ["quality"], "row_counts": counts, "findings": self.classify(out["checks"]),
                  "checks_passed": out["checks_passed"], "checks_failed": out["checks_failed"], "status": out["status"]}
        if out["hard_failed"]:                                            # stop condition
            update.update(status="FAIL", stopped_at="quality", escalated_to_human=True)
        return update

    def analytics(self, state: State) -> dict:
        from src.gold_analytics import run_gold
        run_gold(self.spark)
        out = self.tools.use("analytics", "run_quality_checks", layers=["gold"],
                             previous_completion_rate=self.previous_completion_rate())
        status = "FAIL" if out["hard_failed"] else (
            "PASS_WITH_WARNINGS" if out["checks_failed"] or state.get("status") == "PASS_WITH_WARNINGS" else "PASS")
        update = {"nodes_run": ["analytics"], "findings": self.classify(out["checks"]), "status": status,
                  "checks_passed": out["checks_passed"], "checks_failed": out["checks_failed"]}
        if out["hard_failed"]:
            update.update(stopped_at="analytics", escalated_to_human=True)
        return update

    def explain(self, state: State) -> dict:
        context = {"recent_reports": self.recent_reports(3),
                   "tables": [self.tools.use("explain", "get_table_stats", layer=l) for l in ["bronze", "silver", "quarantine"]]}
        answer = self.llm.ask_json(EXPLAIN_PROMPT, {"question": state["request"], "context": context}).get("answer", "")
        return {"nodes_run": ["explain"], "answer": answer, "status": "ANSWERED"}

    # ---- graph -----------------------------------------------------------------
    def build_graph(self):
        from langgraph.graph import END, START, StateGraph
        g = StateGraph(State)
        for name in ["supervisor", "etl", "quality", "analytics", "explain"]:
            g.add_node(name, getattr(self, name))
        g.add_edge(START, "supervisor")
        g.add_conditional_edges("supervisor", lambda s: s["route"],
                                {"run_etl": "etl", "quality": "quality", "analytics": "analytics", "explain": "explain"})
        g.add_conditional_edges("etl", lambda s: "failure" if s.get("stopped_at") == "etl" else "success",
                                {"success": "quality", "failure": END})
        g.add_conditional_edges("quality", lambda s: "fail" if s.get("stopped_at") == "quality" else "pass",
                                {"pass": "analytics", "fail": END})
        g.add_edge("analytics", END)
        g.add_edge("explain", END)
        return g.compile()

    # ---- run history (reports/ in ADLS) -------------------------------------------
    def recent_reports(self, n: int) -> list:
        dbu = config.get_dbutils(self.spark)
        try:
            files = sorted(dbu.fs.ls(config.REPORTS_PATH), key=lambda f: f.name)[-n:]
        except Exception:
            return []
        return [json.loads(dbu.fs.head(f.path, 100000)) for f in files]

    def previous_completion_rate(self):
        for report in reversed(self.recent_reports(20)):
            if report.get("completion_rate") is not None:
                return report["completion_rate"]
        return None

    # ---- entry point -------------------------------------------------------------
    def run(self, request: str, mode: str = "incremental", start_date: str = "", end_date: str = "") -> dict:
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        started = time.time()
        self.llm.tokens_used = 0
        self.tools.calls = []
        state = self.app.invoke({"run_id": run_id, "request": request, "mode": mode,
                                 "start_date": start_date, "end_date": end_date},
                                config={"recursion_limit": config.AGENT_MAX_STEPS})
        report = {
            "run_id": run_id,
            "route": state.get("route"),
            "nodes_run": state.get("nodes_run", []),
            "status": state.get("status"),
            "row_counts": state.get("row_counts", {}),
            "checks_passed": state.get("checks_passed", 0),
            "checks_failed": state.get("checks_failed", 0),
            "findings": state.get("findings", []),
            "escalated_to_human": bool(state.get("escalated_to_human")),
            "stopped_at": state.get("stopped_at"),
            "answer": state.get("answer"),
            "tokens_used": self.llm.tokens_used,
            "seconds": round(time.time() - started, 1),
            "tool_calls": self.tools.calls,
        }
        if "analytics" in report["nodes_run"]:
            from pyspark.sql import functions as F
            daily = self.spark.read.format("delta").load(config.GOLD_TABLES["daily_bookings"])
            t = daily.agg(F.sum("completed").alias("c"), F.sum("total_bookings").alias("b")).first()
            report["completion_rate"] = round(t["c"] / t["b"], 4)
        report["report_path"] = self.tools.use("explain", "write_report", run_id=run_id, report=report)
        print(json.dumps({k: v for k, v in report.items() if k != "tool_calls"}, indent=2, default=str))
        return report
