import boto3

# Crear una sesión Boto3
# Cambia la región si es necesario
session = boto3.Session(region_name='us-east-1')

# Crear un cliente para AWS Glue
glue_client = session.client('glue')

# Nombre del job que quieres ejecutar
job_name = 'JobSakilaParcial'  # Cambia esto por el nombre de tu job

try:
    # Iniciar el job
    response = glue_client.start_job_run(JobName=job_name)

    # Obtener el ID de ejecución del job
    job_run_id = response['JobRunId']

    print(f"Job {job_name} iniciado con éxito. Job Run ID: {job_run_id}")

except Exception as e:
    print(f"Error al iniciar el job {job_name}: {e}")