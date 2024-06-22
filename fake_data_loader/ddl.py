PG_TABLES = {
    "users": """
        CREATE TABLE IF NOT EXISTS users (
            id uuid default uuid_generate_v4() primary key,
            first_name varchar not null,
            last_name varchar not null,
            email varchar,
            dob date not null,
            state varchar,
            modified_at timestamp without time zone not null,
            created_at timestamp without time zone not null
        );
    """,
    "applications": """
        CREATE TABLE IF NOT EXISTS applications (
            id varchar default uuid_generate_v4() primary key,
            user_id uuid references users(id),
            status varchar not null,
            modified_at timestamp without time zone not null,
            created_at timestamp without time zone not null
        );
    """,
    "balances": """
        CREATE TABLE IF NOT EXISTS balances (
            id varchar default uuid_generate_v4() primary key,
            user_id uuid references users(id),
            amount numeric not null,
            modified_at timestamp without time zone not null,
            created_at timestamp without time zone not null
        );
    """,
    "withdrawals": """
        CREATE TABLE IF NOT EXISTS withdrawals (
            id varchar default uuid_generate_v4() primary key,
            user_id uuid references users(id),
            amount numeric not null,
            created_at timestamp without time zone not null
        );
    """,
    "deposits": """
        CREATE TABLE IF NOT EXISTS deposits (
            id varchar default uuid_generate_v4() primary key,
            user_id uuid references users(id),
            amount numeric not null,
            created_at timestamp without time zone not null
        );
    """
}
