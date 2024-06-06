SELECT 'CREATE DATABASE oltpdatabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'oltpdatabase')\gexec

SELECT 'CREATE DATABASE olapwarehouse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'olapwarehouse')\gexec