import json
import snowflake.connector as sf
import boto3
from tabulate import tabulate

def publish_to_sns(sub, msg):
    topic_arn = "<TOPIC ARN>"
    sns = boto3.client("sns", region_name="<REGION>")
    response = sns.publish(
        TopicArn=topic_arn,
        Message=(msg),
        Subject=(sub)
    )
    print(response)
    print("sent!")

def run_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()

def lambda_handler(event, context):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name='<REGION>')
    get_secret_value_response = client.get_secret_value(SecretId=<SECRET_NAME>)
    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)
    user = secret.get('user')
    password = secret.get('password')
    account = secret.get('account')
    database = secret.get('database')
    warehouse = '<WAREHOUSE>'
    schema = '<SCHEMA>'
    role = '<ROLE>'
    conn = sf.connect(user=user, password=password, account=account);
    cur = conn.cursor()
    results = cur.execute("select COL1,COL2 from <DATABASE>.<SCHEMA>.<TABLE_NAME>").fetchall()

    table_array = []
    headers = ["COL1", "COL2"]

    if results:
        for rec in results:
            row_array = []
            row_array.append(str(rec[0]))
            row_array.append(str(rec[1]))
            table_array.append(row_array)

        strMessage = """Hi Team,\n\nDetails to be noted in today's Snowflake load\n\n"""
        strMessage = strMessage + tabulate(table_array, headers, tablefmt="psql")
        print(strMessage)
        strMessage = strMessage + """\n-sent via "xxxx" lambda"""
        publish_to_sns("Message-Snowflake Lambda Integration", strMessage)
    else:
        print("no violations")