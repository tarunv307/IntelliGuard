-- ==============================================================================
-- IntelliGuard — Supabase (PostgreSQL) Database Schema
-- Run this in the Supabase SQL Editor to create the required tables and indexes.
-- ==============================================================================

-- 1. Datasets Table: Tracks uploaded CSV files & their S3 locations
CREATE TABLE IF NOT EXISTS public.datasets (
    id VARCHAR(64) PRIMARY KEY,
    filename TEXT NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    s3_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Jobs Table: Tracks streaming ingestion progress across restarts
CREATE TABLE IF NOT EXISTS public.jobs (
    job_id VARCHAR(64) PRIMARY KEY,
    dataset_id VARCHAR(64) NOT NULL REFERENCES public.datasets(id) ON DELETE CASCADE,
    filename TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'queued', -- queued, loading, complete, failed
    rows_total INTEGER NOT NULL DEFAULT 0,
    rows_loaded INTEGER NOT NULL DEFAULT 0,
    rows_failed INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fast job lookup
CREATE INDEX IF NOT EXISTS idx_jobs_dataset_id ON public.jobs(dataset_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON public.jobs(status);

-- 3. Chat Audit Logs Table: Stores questions, generated Cypher, and grounded status
CREATE TABLE IF NOT EXISTS public.chat_logs (
    id BIGSERIAL PRIMARY KEY,
    dataset_id VARCHAR(64),
    question TEXT NOT NULL,
    answer TEXT NOT NULL,
    cypher TEXT,
    grounded BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for analytics & fast search
CREATE INDEX IF NOT EXISTS idx_chat_logs_created_at ON public.chat_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_chat_logs_grounded ON public.chat_logs(grounded);

-- Enable Row Level Security (RLS) & allow public/authenticated access
ALTER TABLE public.datasets ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.chat_logs ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all access to datasets for service" ON public.datasets FOR ALL USING (true);
CREATE POLICY "Allow all access to jobs for service" ON public.jobs FOR ALL USING (true);
CREATE POLICY "Allow all access to chat_logs for service" ON public.chat_logs FOR ALL USING (true);
