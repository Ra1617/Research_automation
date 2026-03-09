import json
import re


def safe_parse(raw: str) -> dict:
    """Parse raw LLM output into a dict, handling common formatting issues."""
    if not raw:
        return {}
    
    # Strip markdown code fences
    clean = raw.strip()
    clean = re.sub(r'^```(?:json)?\s*', '', clean)
    clean = re.sub(r'\s*```$', '', clean)
    clean = clean.strip()
    
    # Find the first { and last } to extract JSON object
    start = clean.find('{')
    end = clean.rfind('}')
    if start != -1 and end != -1:
        clean = clean[start:end+1]
    
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        # Try fixing common issues: trailing commas
        clean = re.sub(r',\s*}', '}', clean)
        clean = re.sub(r',\s*]', ']', clean)
        try:
            return json.loads(clean)
        except json.JSONDecodeError:
            return {}
