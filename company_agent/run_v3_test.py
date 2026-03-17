"""Quick end-to-end test — writes results to output.txt for readability."""
import json, traceback, sys, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# switch to workspace directory so imports work
out = open("v3_test_output.txt", "w", encoding="utf-8")

# convenience helper for writing log messages (used only for failure cases)
def log(msg):
    print(msg)
    out.write(msg + "\n")
    out.flush()

log("=" * 60)
log("  v3 Pipeline — End-to-End Test")
log("=" * 60)

# in v3 the FastAPI server has been removed; call the CLI helper directly
try:
    # use run_company which returns a dict suitable for JSON serialization
    from v3.main import run_company, save_output_json
    log("[OK] Imports succeeded")
except Exception as e:
    log(f"[FAIL] Import error: {e}")
    traceback.print_exc(file=out)
    out.close()
    sys.exit(1)

# run a full pipeline and capture results as JSON
log("\n--- executing pipeline (company: OpenAI) ---")
try:
    resp = run_company("OpenAI")
    # write pretty JSON both to stdout/file and dedicated output file
    json_text = json.dumps(resp, indent=2, default=str)
    print(json_text)
    output_path = save_output_json(resp, "OpenAI")
    log(f"[DONE] JSON output written to {output_path}")
except Exception as e:
    log(f"EXCEPTION: {e}")
    traceback.print_exc(file=out)

log("\nDONE")
out.close()
