SELECT id, company_name, model_name, agent_stage, created_at
FROM raw_model_outputs
ORDER BY created_at DESC
LIMIT 20;

SELECT company_name, COUNT(*) AS raw_rows
FROM raw_model_outputs
GROUP BY company_name
ORDER BY raw_rows DESC;

SELECT id, company_name, consolidation_model, agent_stage, created_at
FROM consolidated_outputs
ORDER BY created_at DESC
LIMIT 20;

SELECT company_name, consolidated_json
FROM consolidated_outputs
WHERE company_name = 'OpenAI'
ORDER BY created_at DESC
LIMIT 1;

SELECT company_name, model_name, raw_json
FROM raw_model_outputs
WHERE company_name = 'OpenAI'
ORDER BY created_at DESC;
