-- ============================================================
-- CLÍNICA ROSELI ANDRADE — Tracking WhatsApp + Google Ads
-- Supabase Setup Completo
-- Projeto: xjngdyhzpgktddxfmdgq
-- ============================================================

-- 1. ENUM de status do funil
CREATE TYPE conversion_status AS ENUM (
  'clique',
  'conversa',
  'agendamento',
  'compareceu',
  'nao_compareceu'
);

-- 2. TABELA PRINCIPAL: wa_conversions
CREATE TABLE wa_conversions (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  protocolo TEXT UNIQUE NOT NULL,          -- ex: RA-3K7X9
  visitor_id TEXT,                          -- ID anônimo do visitante
  nome TEXT,                               -- Nome do paciente
  phone TEXT,                              -- Telefone
  status conversion_status DEFAULT 'clique',
  gclid TEXT,                              -- Google Click ID
  utm_source TEXT,
  utm_medium TEXT,
  utm_campaign TEXT,
  utm_term TEXT,
  utm_content TEXT,
  procedimento TEXT,                       -- botox, peeling, rinoplastia, etc
  valor_consulta DECIMAL(10,2) DEFAULT 0,  -- Valor em R$
  notas TEXT,                              -- Observações do atendente
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- 3. TABELA DE LOG: conversion_status_log (auditoria)
CREATE TABLE conversion_status_log (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  conversion_id UUID REFERENCES wa_conversions(id) ON DELETE CASCADE,
  status_anterior conversion_status,
  status_novo conversion_status NOT NULL,
  changed_at TIMESTAMPTZ DEFAULT now()
);

-- 4. TABELA: ga_offline_uploads (controle de uploads Google Ads)
CREATE TABLE ga_offline_uploads (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  upload_date TIMESTAMPTZ DEFAULT now(),
  conversions_count INTEGER DEFAULT 0,
  status TEXT DEFAULT 'pending',           -- pending, uploaded, error
  file_name TEXT,
  notas TEXT
);

-- ============================================================
-- ÍNDICES
-- ============================================================

CREATE INDEX idx_wa_conversions_gclid ON wa_conversions(gclid);
CREATE INDEX idx_wa_conversions_status ON wa_conversions(status);
CREATE INDEX idx_wa_conversions_created ON wa_conversions(created_at);
CREATE INDEX idx_wa_conversions_protocolo ON wa_conversions(protocolo);
CREATE INDEX idx_wa_conversions_campaign ON wa_conversions(utm_campaign);
CREATE INDEX idx_status_log_conversion ON conversion_status_log(conversion_id);

-- ============================================================
-- TRIGGERS
-- ============================================================

-- Trigger 1: Atualizar updated_at automaticamente
CREATE OR REPLACE FUNCTION fn_update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_timestamp
  BEFORE UPDATE ON wa_conversions
  FOR EACH ROW
  EXECUTE FUNCTION fn_update_timestamp();

-- Trigger 2: Log de transição de status
CREATE OR REPLACE FUNCTION fn_log_status_change()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.status IS DISTINCT FROM NEW.status THEN
    INSERT INTO conversion_status_log (conversion_id, status_anterior, status_novo)
    VALUES (NEW.id, OLD.status, NEW.status);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_log_status_change
  AFTER UPDATE ON wa_conversions
  FOR EACH ROW
  EXECUTE FUNCTION fn_log_status_change();

-- Trigger 3: Log inicial na criação
CREATE OR REPLACE FUNCTION fn_log_initial_status()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO conversion_status_log (conversion_id, status_anterior, status_novo)
  VALUES (NEW.id, NULL, NEW.status);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_log_initial_status
  AFTER INSERT ON wa_conversions
  FOR EACH ROW
  EXECUTE FUNCTION fn_log_initial_status();

-- Trigger 4: Validação de transição (impede pular etapas)
CREATE OR REPLACE FUNCTION fn_validate_status_transition()
RETURNS TRIGGER AS $$
BEGIN
  -- Transições permitidas:
  -- clique -> conversa
  -- conversa -> agendamento
  -- agendamento -> compareceu | nao_compareceu
  -- nao_compareceu -> agendamento (remarcação)
  IF OLD.status = 'clique' AND NEW.status NOT IN ('conversa') THEN
    RAISE EXCEPTION 'Transição inválida: % -> %', OLD.status, NEW.status;
  END IF;
  IF OLD.status = 'conversa' AND NEW.status NOT IN ('agendamento') THEN
    RAISE EXCEPTION 'Transição inválida: % -> %', OLD.status, NEW.status;
  END IF;
  IF OLD.status = 'agendamento' AND NEW.status NOT IN ('compareceu', 'nao_compareceu') THEN
    RAISE EXCEPTION 'Transição inválida: % -> %', OLD.status, NEW.status;
  END IF;
  IF OLD.status = 'nao_compareceu' AND NEW.status NOT IN ('agendamento') THEN
    RAISE EXCEPTION 'Transição inválida: % -> %', OLD.status, NEW.status;
  END IF;
  IF OLD.status = 'compareceu' THEN
    RAISE EXCEPTION 'Status "compareceu" é final, não pode ser alterado';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validate_status_transition
  BEFORE UPDATE OF status ON wa_conversions
  FOR EACH ROW
  EXECUTE FUNCTION fn_validate_status_transition();

-- ============================================================
-- VIEWS
-- ============================================================

-- View 1: CSV para Google Ads (formato de upload offline)
CREATE OR REPLACE VIEW v_google_ads_csv AS
SELECT
  gclid AS "Google Click ID",
  'Agendamento WhatsApp' AS "Conversion Name",
  TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS') AS "Conversion Time",
  '0.00' AS "Conversion Value",
  'BRL' AS "Conversion Currency"
FROM wa_conversions
WHERE status = 'agendamento' AND gclid IS NOT NULL;

-- View 2: CSV para Google Ads (consulta realizada com valor)
CREATE OR REPLACE VIEW v_google_ads_consulta AS
SELECT
  gclid AS "Google Click ID",
  'Consulta Realizada' AS "Conversion Name",
  TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS') AS "Conversion Time",
  COALESCE(valor_consulta, 0)::TEXT AS "Conversion Value",
  'BRL' AS "Conversion Currency"
FROM wa_conversions
WHERE status = 'compareceu' AND gclid IS NOT NULL;

-- View 3: Funil completo
CREATE OR REPLACE VIEW v_funil AS
SELECT
  COUNT(*) FILTER (WHERE status IN ('clique','conversa','agendamento','compareceu','nao_compareceu')) AS cliques,
  COUNT(*) FILTER (WHERE status IN ('conversa','agendamento','compareceu','nao_compareceu')) AS conversas,
  COUNT(*) FILTER (WHERE status IN ('agendamento','compareceu','nao_compareceu')) AS agendamentos,
  COUNT(*) FILTER (WHERE status = 'compareceu') AS compareceram,
  COUNT(*) FILTER (WHERE status = 'nao_compareceu') AS nao_compareceram,
  CASE WHEN COUNT(*) > 0
    THEN ROUND(100.0 * COUNT(*) FILTER (WHERE status IN ('agendamento','compareceu','nao_compareceu')) / COUNT(*), 1)
    ELSE 0
  END AS taxa_agendamento_pct,
  CASE WHEN COUNT(*) FILTER (WHERE status IN ('agendamento','compareceu','nao_compareceu')) > 0
    THEN ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'compareceu') / COUNT(*) FILTER (WHERE status IN ('agendamento','compareceu','nao_compareceu')), 1)
    ELSE 0
  END AS taxa_comparecimento_pct
FROM wa_conversions;

-- View 4: Performance por campanha
CREATE OR REPLACE VIEW v_performance_campanha AS
SELECT
  COALESCE(utm_campaign, '(sem campanha)') AS campanha,
  COUNT(*) AS total_cliques,
  COUNT(*) FILTER (WHERE status IN ('conversa','agendamento','compareceu','nao_compareceu')) AS conversas,
  COUNT(*) FILTER (WHERE status IN ('agendamento','compareceu','nao_compareceu')) AS agendamentos,
  COUNT(*) FILTER (WHERE status = 'compareceu') AS compareceram,
  SUM(valor_consulta) FILTER (WHERE status = 'compareceu') AS receita_total
FROM wa_conversions
GROUP BY utm_campaign
ORDER BY agendamentos DESC;

-- View 5: Tendência diária
CREATE OR REPLACE VIEW v_tendencia_diaria AS
SELECT
  DATE(created_at) AS dia,
  COUNT(*) AS cliques,
  COUNT(*) FILTER (WHERE status IN ('agendamento','compareceu','nao_compareceu')) AS agendamentos,
  COUNT(*) FILTER (WHERE status = 'compareceu') AS compareceram
FROM wa_conversions
GROUP BY DATE(created_at)
ORDER BY dia DESC;

-- View 6: Performance por procedimento
CREATE OR REPLACE VIEW v_performance_procedimento AS
SELECT
  COALESCE(procedimento, '(não informado)') AS procedimento,
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE status = 'compareceu') AS compareceram,
  SUM(valor_consulta) FILTER (WHERE status = 'compareceu') AS receita,
  ROUND(AVG(valor_consulta) FILTER (WHERE status = 'compareceu'), 2) AS ticket_medio
FROM wa_conversions
GROUP BY procedimento
ORDER BY receita DESC NULLS LAST;

-- View 7: Leads recentes
CREATE OR REPLACE VIEW v_leads_recentes AS
SELECT
  protocolo,
  nome,
  phone,
  status,
  procedimento,
  utm_campaign,
  valor_consulta,
  created_at,
  updated_at
FROM wa_conversions
ORDER BY created_at DESC
LIMIT 50;

-- ============================================================
-- RLS (Row Level Security)
-- ============================================================

-- Habilitar RLS
ALTER TABLE wa_conversions ENABLE ROW LEVEL SECURITY;
ALTER TABLE conversion_status_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE ga_offline_uploads ENABLE ROW LEVEL SECURITY;

-- Policy: Bridge Page (anon) só pode INSERIR com status 'clique'
CREATE POLICY "bridge_page_insert"
  ON wa_conversions
  FOR INSERT
  TO anon
  WITH CHECK (status = 'clique');

-- Policy: anon NÃO pode ler, atualizar ou deletar
-- (nenhuma policy SELECT/UPDATE/DELETE para anon = bloqueado)

-- Policy: authenticated pode tudo (dashboard)
CREATE POLICY "dashboard_full_access"
  ON wa_conversions
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

-- Policy: service_role pode tudo (admin/scripts)
CREATE POLICY "service_role_full"
  ON wa_conversions
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- Log: só authenticated e service_role podem ler
CREATE POLICY "log_read_authenticated"
  ON conversion_status_log
  FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "log_read_service"
  ON conversion_status_log
  FOR SELECT
  TO service_role
  USING (true);

-- Uploads: só authenticated e service_role
CREATE POLICY "uploads_full_authenticated"
  ON ga_offline_uploads
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "uploads_full_service"
  ON ga_offline_uploads
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- ============================================================
-- DONE! 🎯
-- 3 tabelas, 6 índices, 4 triggers, 7 views, RLS completo
-- ============================================================
