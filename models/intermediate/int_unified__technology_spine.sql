-- =============================================================================
-- Technology Spine: Maps technology names across all 3 sources
-- This is the Rosetta Stone of the project. Each source uses slightly different
-- names (e.g., "JavaScript" vs "Javascript", "C++" vs "Cpp", "Node.js" vs "NodeJS")
-- =============================================================================

with technology_mapping as (
    -- Canonical mapping: source_name -> canonical_name + metadata
    -- This seed-like CTE acts as the single source of truth for name resolution
    select * from (values
        -- === Programming Languages ===
        ('JavaScript',      'JavaScript',   'language',       'web'),
        ('Javascript',      'JavaScript',   'language',       'web'),
        ('TypeScript',      'TypeScript',   'language',       'web'),
        ('Typescript',      'TypeScript',   'language',       'web'),
        ('Python',          'Python',       'language',       'general'),
        ('Java',            'Java',         'language',       'general'),
        ('C#',              'C#',           'language',       'general'),
        ('Csharp',          'C#',           'language',       'general'),
        ('C++',             'C++',          'language',       'systems'),
        ('Cpp',             'C++',          'language',       'systems'),
        ('C',               'C',            'language',       'systems'),
        ('PHP',             'PHP',          'language',       'web'),
        ('Ruby',            'Ruby',         'language',       'web'),
        ('Go',              'Go',           'language',       'systems'),
        ('Golang',          'Go',           'language',       'systems'),
        ('Rust',            'Rust',         'language',       'systems'),
        ('Swift',           'Swift',        'language',       'mobile'),
        ('Kotlin',          'Kotlin',       'language',       'mobile'),
        ('Dart',            'Dart',         'language',       'mobile'),
        ('Scala',           'Scala',        'language',       'data'),
        ('R',               'R',            'language',       'data'),
        ('MATLAB',          'MATLAB',       'language',       'data'),
        ('Lua',             'Lua',          'language',       'scripting'),
        ('Perl',            'Perl',         'language',       'scripting'),
        ('Shell',           'Shell',        'language',       'scripting'),
        ('Bash/Shell',      'Shell',        'language',       'scripting'),
        ('PowerShell',      'PowerShell',   'language',       'scripting'),
        ('SQL',             'SQL',          'language',       'data'),
        ('HTML/CSS',        'HTML/CSS',     'language',       'web'),
        ('Elixir',          'Elixir',       'language',       'general'),
        ('Clojure',         'Clojure',      'language',       'general'),
        ('Haskell',         'Haskell',      'language',       'general'),
        ('Julia',           'Julia',        'language',       'data'),
        ('Zig',             'Zig',          'language',       'systems'),
        ('Assembly',        'Assembly',     'language',       'systems'),
        ('Objective-C',     'Objective-C',  'language',       'mobile'),

        -- === Web Frameworks ===
        ('React',           'React',        'web_framework',  'frontend'),
        ('React.js',        'React',        'web_framework',  'frontend'),
        ('Angular',         'Angular',      'web_framework',  'frontend'),
        ('Vue.js',          'Vue.js',       'web_framework',  'frontend'),
        ('Vue',             'Vue.js',       'web_framework',  'frontend'),
        ('Svelte',          'Svelte',       'web_framework',  'frontend'),
        ('Next.js',         'Next.js',      'web_framework',  'fullstack'),
        ('NextJS',          'Next.js',      'web_framework',  'fullstack'),
        ('Nuxt.js',         'Nuxt.js',      'web_framework',  'fullstack'),
        ('Django',          'Django',       'web_framework',  'backend'),
        ('Flask',           'Flask',        'web_framework',  'backend'),
        ('FastAPI',         'FastAPI',      'web_framework',  'backend'),
        ('Express',         'Express.js',   'web_framework',  'backend'),
        ('Express.js',      'Express.js',   'web_framework',  'backend'),
        ('Spring',          'Spring',       'web_framework',  'backend'),
        ('Spring Boot',     'Spring Boot',  'web_framework',  'backend'),
        ('Ruby on Rails',   'Rails',        'web_framework',  'backend'),
        ('Rails',           'Rails',        'web_framework',  'backend'),
        ('Laravel',         'Laravel',      'web_framework',  'backend'),
        ('ASP.NET',         'ASP.NET',      'web_framework',  'backend'),
        ('ASP.NET Core',    'ASP.NET Core', 'web_framework',  'backend'),
        ('Node.js',         'Node.js',      'platform',       'backend'),
        ('NodeJS',          'Node.js',      'platform',       'backend'),
        ('Deno',            'Deno',         'platform',       'backend'),
        ('Bun',             'Bun',          'platform',       'backend'),
        ('jQuery',          'jQuery',       'web_framework',  'frontend'),
        ('htmx',            'htmx',         'web_framework',  'frontend'),

        -- === Databases ===
        ('PostgreSQL',      'PostgreSQL',   'database',       'relational'),
        ('Postgres',        'PostgreSQL',   'database',       'relational'),
        ('MySQL',           'MySQL',        'database',       'relational'),
        ('SQLite',          'SQLite',       'database',       'relational'),
        ('Microsoft SQL Server', 'SQL Server', 'database',   'relational'),
        ('SQL Server',      'SQL Server',   'database',       'relational'),
        ('Oracle',          'Oracle DB',    'database',       'relational'),
        ('MongoDB',         'MongoDB',      'database',       'nosql'),
        ('Redis',           'Redis',        'database',       'nosql'),
        ('Elasticsearch',   'Elasticsearch','database',       'nosql'),
        ('DynamoDB',        'DynamoDB',     'database',       'nosql'),
        ('Firebase',        'Firebase',     'database',       'nosql'),
        ('Cassandra',       'Cassandra',    'database',       'nosql'),
        ('Neo4j',           'Neo4j',        'database',       'nosql'),
        ('Snowflake',       'Snowflake',    'database',       'cloud_dw'),
        ('BigQuery',        'BigQuery',     'database',       'cloud_dw'),
        ('Databricks',      'Databricks',   'database',       'cloud_dw'),
        ('ClickHouse',      'ClickHouse',   'database',       'analytics'),

        -- === Platforms / Cloud ===
        ('AWS',             'AWS',          'platform',       'cloud'),
        ('Amazon Web Services', 'AWS',      'platform',       'cloud'),
        ('Azure',           'Azure',        'platform',       'cloud'),
        ('Microsoft Azure', 'Azure',        'platform',       'cloud'),
        ('Google Cloud',    'GCP',          'platform',       'cloud'),
        ('Google Cloud Platform', 'GCP',    'platform',       'cloud'),
        ('Docker',          'Docker',       'platform',       'devops'),
        ('Kubernetes',      'Kubernetes',   'platform',       'devops'),

        -- === GitHub language mapping (GH uses title-case repo language) ===
        ('JavaScript',      'JavaScript',   'language',       'web'),
        ('TypeScript',      'TypeScript',   'language',       'web'),
        ('Python',          'Python',       'language',       'general'),
        ('Java',            'Java',         'language',       'general'),
        ('C#',              'C#',           'language',       'general'),
        ('C++',             'C++',          'language',       'systems'),
        ('Go',              'Go',           'language',       'systems'),
        ('Rust',            'Rust',         'language',       'systems'),
        ('Ruby',            'Ruby',         'language',       'web'),
        ('PHP',             'PHP',          'language',       'web'),
        ('Swift',           'Swift',        'language',       'mobile'),
        ('Kotlin',          'Kotlin',       'language',       'mobile'),
        ('Dart',            'Dart',         'language',       'mobile'),
        ('Scala',           'Scala',        'language',       'data'),
        ('Shell',           'Shell',        'language',       'scripting'),
        ('Lua',             'Lua',          'language',       'scripting'),
        ('Elixir',          'Elixir',       'language',       'general'),
        ('Haskell',         'Haskell',      'language',       'general'),
        ('Zig',             'Zig',          'language',       'systems'),
        ('Jupyter Notebook','Python',       'language',       'data'),
        ('HCL',             'HCL',          'language',       'devops'),
        ('Vue',             'Vue.js',       'web_framework',  'frontend')

    ) as t(source_name, canonical_name, technology_category, technology_subcategory)
),

-- Deduplicate the mapping (same source_name might appear multiple times)
deduped_mapping as (
    select
        source_name,
        canonical_name,
        technology_category,
        technology_subcategory,
        row_number() over (
            partition by source_name
            order by canonical_name
        ) as rn
    from technology_mapping
    qualify rn = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['canonical_name', 'technology_category']) }}
        as technology_id,
    source_name,
    canonical_name,
    technology_category,
    technology_subcategory
from deduped_mapping
