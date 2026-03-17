CREATE TABLE IF NOT EXISTS raw_model_outputs (
    id UUID PRIMARY KEY,
    company_name TEXT NOT NULL,
    model_name TEXT NOT NULL,
    agent_stage TEXT NOT NULL,
    raw_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS consolidated_outputs (
    id UUID PRIMARY KEY,
    company_name TEXT NOT NULL,
    consolidation_model TEXT NOT NULL,
    agent_stage TEXT NOT NULL,
    consolidated_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_model_outputs_company_name
    ON raw_model_outputs (company_name);

CREATE INDEX IF NOT EXISTS idx_raw_model_outputs_model_name
    ON raw_model_outputs (model_name);

CREATE INDEX IF NOT EXISTS idx_consolidated_outputs_company_name
    ON consolidated_outputs (company_name);
