Schema/Relationships of the generated tables

######## Tables ######## 

Users - Users who have signed up to the platform.
- id, uuid, primary key
- first name, string, non-nullable
- last name, string, non-nullable
- email, string, nullable
- dob, date, non-nullable
- state, string, nullable
- modified_at, timestampntz (UTC), non-nullable
- created_at, timestampntz (UTC), non-nullable

Applications - User applications to open an account after signup.
- id, uuid, primary key
- user_id, uuid, foreign key (users__id) 1-1
- status, string, non-nullable
- modified_at, timestampntz (UTC), non-nullable
- created_at, timestampntz (UTC), non-nullable

Balances - User dollar amount balances.
- id, uuid, primary key
- user_id, uuid, foreign key (users__id) 1-1
- amount, decimal, non-nullable
- modified_at, timestampntz (UTC), non-nullable
- created_at, timestampntz (UTC), non-nullable

Withdrawals - User 
- id, uuid, primary key
- user_id, uuid, foreign key (users__id) 1-1
- amount, decimal
- created_at, timestampntz (UTC), non-nullable

Deposits - User 
- id, uuid, primary key
- user_id, uuid, foreign key (users__id) 1-1
- amount, decimal
- created_at, timestampntz (UTC), non-nullable

######## Events ########

Event Name: "user sign up"
Payload:
{
    "event": "user signup"
    "event_ts": timestampntz (UTC),
    "first_name": string,
    "last_name": string,
    "email": string,
    "dob": date,
    "state": string
}
Prerequisites: None

--

Event Name: "user update demographic"
Payload:
{
    "event": "user location update"
    "event_ts": timestampntz (UTC),
    "id": uuid,
    "state": string
}
Prerequisites:
    - user exist
    - `state` value is different from old.
Post Actions:
    - update `state` in `users` table.

--

Event Name: "user application open"
Payload:
{
    "event": "user application open"
    "event_ts": timestampntz (UTC),
    "user_id": uuid,
    "status": "pending"
}
Prerequisites:
    - user exist
    - user does not have an existing application
Post Actions:
    - update `status` in `application` table.

--

Event Name: "user application reject"
Payload:
{
    "event": "user application reject"
    "event_ts": timestampntz (UTC),
    "id": uuid,
    "user_id": uuid
}
Prerequisites:
    - user has application with status `open`
Post Actions:
    - update `status` in `application` table.
    - update `modified_at` in `application` table.

--

Event Name: "user application approve"
Payload:
{
    "event": "user application approve"
    "event_ts": timestampntz (UTC),
    "id": uuid,
    "user_id": uuid
}
Prerequisites:
    - user has application with status `open`
Post Actions:
    - initialize zero balance row in `balance` table.
    - update `status` in `application` table.
    - update `modified_at` in `application` table.

--

Event Name: "user deposit"
Payload:
{
    "event": "user deposit"
    "event_ts": timestampntz (UTC),
    "user_id": uuid,
    "amount": decimal
}
Prerequisites:
    - user has application with status `approve`
Post Actions:
    - update `amount` in `balance` table.

--

Event Name: "user withdraw"
Payload:
{
    "event": "user withdraw"
    "event_ts": timestampntz (UTC),
    "id": uuid,
    "user_id": uuid,
    "amount": decimal
}
Prerequisites:
    - user has sufficient `amount` in `balance` table.
Post Actions:
    - update `amount` in `balance` table.
