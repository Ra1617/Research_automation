import time
from models.codestral_client import query_codestral
from models.mistral_client import query_mistral
from services.consensus_engine import consolidate
from config import CHUNK_SIZE, MAX_RETRIES


QUERY_FUNCS = {
    "Codestral": query_codestral,
    # "Groq": query_groq,
    "Mistral": query_mistral
}


def build_prompt(company_name: str, param_chunk: list) -> str:
    lines = []
    for p in param_chunk:
        nullable = "can be null" if p["nullability"] == "Nullable" else "REQUIRED"
        lines.append(f'  "{p["key"]}": // {p["description"]} | type: {p["data_type"]} | {nullable}')
    params_block = "\n".join(lines)

    return f"""Research the company: "{company_name}"

Return a JSON object with EXACTLY these keys. Follow the type and nullability hints:

{{
{params_block}
}}

RULES:
- Return ONLY the JSON object above — nothing else
- Use null for any field you cannot find reliable data for
- Do NOT hallucinate or guess — only use verifiable facts
- All string values must be properly escaped
"""


def query_with_retry(model_name: str, prompt: str, retries: int = MAX_RETRIES) -> str:
    fn = QUERY_FUNCS[model_name]
    for attempt in range(1, retries + 1):
        try:
            return fn(prompt)
        except Exception as e:
            print(f"   [{model_name}] Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)  # exponential backoff
    return "{}"  # return empty JSON on total failure


def run_pipeline(company_name: str, all_params: list) -> tuple[dict, list]:
    """
    Main pipeline: chunks params, queries all 3 models, consolidates.
    Returns (final_result_dict, raw_outputs_list)
    """
    chunks = [all_params[i:i+CHUNK_SIZE] for i in range(0, len(all_params), CHUNK_SIZE)]
    final_result = {}
    raw_outputs = []

    print(f"\n Processing {len(all_params)} parameters in {len(chunks)} chunks")
    print(f" Models: {', '.join(QUERY_FUNCS.keys())}\n")

    for i, chunk in enumerate(chunks, 1):
        print(f"[Chunk {i:02d}/{len(chunks)}] Params {chunk[0]['id']}–{chunk[-1]['id']}: {chunk[0]['key']} → {chunk[-1]['key']}")
        prompt = build_prompt(company_name, chunk)
        
        chunk_outputs = {}
        for model_name in QUERY_FUNCS:
            print(f"   → Querying {model_name}...", end=" ", flush=True)
            raw = query_with_retry(model_name, prompt)
            chunk_outputs[model_name] = raw
            print("done")
        
        raw_outputs.append({"chunk": i, "params": [p["key"] for p in chunk], "raw": chunk_outputs})
        
        chunk_result = consolidate(chunk_outputs, chunk)
        final_result.update(chunk_result)
        
        # Brief stats for this chunk
        high = sum(1 for v in chunk_result.values() if v["confidence"] == "high")
        low  = sum(1 for v in chunk_result.values() if v["confidence"] == "low")
        none = sum(1 for v in chunk_result.values() if v["confidence"] == "none")
        print(f"   ✓ Consensus: {high} high | {low} low | {none} no-data\n")
        
        time.sleep(0.5)  # be polite to APIs

    return final_result, raw_outputs
