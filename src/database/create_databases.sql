SELECT 'CREATE DATABASE oltpdatabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'oltpdatabase')\gexec

SELECT 'CREATE DATABASE olapwarehouse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'olapwarehouse')\gexec

SELECT 'CREATE DATABASE testoltpdatabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'testoltpdatabase')\gexec

SELECT 'CREATE DATABASE testolapwarehouse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'testolapwarehouse')\gexec
