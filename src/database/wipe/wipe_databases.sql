SELECT 'DROP DATABASE oltpdatabase'
WHERE EXISTS (SELECT FROM pg_database WHERE datname = 'oltpdatabase')\gexec

SELECT 'DROP DATABASE olapwarehouse'
WHERE EXISTS (SELECT FROM pg_database WHERE datname = 'olapwarehouse')\gexec

SELECT 'DROP DATABASE testoltpdatabase'
WHERE EXISTS (SELECT FROM pg_database WHERE datname = 'testoltpdatabase')\gexec

SELECT 'DROP DATABASE testolapwarehouse'
WHERE EXISTS (SELECT FROM pg_database WHERE datname = 'testolapwarehouse')\gexec
