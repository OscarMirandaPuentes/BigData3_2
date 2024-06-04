import boto3

# Crear una sesión predeterminada de Boto3
session = boto3.Session(region_name='us-east-1')
glue_client = session.client('glue')

# Parámetros del crawler
crawler_name = 'datawarehouse'
role = 'arn:aws:iam::058264114657:role/LabRole'
database_name = 'datawarehouse_sakila'
s3_target_path = 's3://sakila-warehouse-2/final/'

# Crear un database en Glue si no existe


def create_database():
    try:
        response = glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Database for Sakila data warehouse tables',
                'LocationUri': s3_target_path
            }
        )
        print(f"Database {database_name} created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Database {database_name} already exists.")

# Crear un crawler


def create_crawler():
    try:
        response = glue_client.create_crawler(
            Name=crawler_name,
            Role=role,
            DatabaseName=database_name,
            Targets={
                'S3Targets': [
                    {
                        'Path': s3_target_path,
                        'Exclusions': []
                    }
                ]
            },
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'LOG'
            },
            RecrawlPolicy={
                'RecrawlBehavior': 'CRAWL_EVERYTHING'
            }
        )
        print(f"Crawler {crawler_name} created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Crawler {crawler_name} already exists.")

# Iniciar el crawler


def start_crawler():
    response = glue_client.start_crawler(Name=crawler_name)
    print(f"Crawler {crawler_name} started.")


# Ejecutar las funciones
create_database()
create_crawler()
start_crawler()