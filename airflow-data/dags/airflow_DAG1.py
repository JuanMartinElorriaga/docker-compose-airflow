from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.contrib.operators.neo4j_operator import Neo4jOperator
from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from py2neo import Graph, Node, Relationship
#from lxml import etree
import xml.dom.minidom as minidom
from os import environ
from dotenv import load_dotenv
load_dotenv()

# Default arguments for the DAG
default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'start_date'      : datetime(2023, 3, 18),
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 0,
    'retry_delay'     : timedelta(minutes=2),
}

# Define the DAG object
#dag = DAG(
#    'uniprot_neo4j_pipeline',
#    default_args      = default_args,
#    schedule_interval = timedelta(days=1),
#)

neo4j_conn = BaseHook.get_connection("neo4j_default")

def download_uniprot_xml():
    # Download file to /tmp directory using curl command
    import subprocess
    subprocess.call(['curl', '-o', '/tmp/Q9Y261.xml', 'https://github.com/JuanMartinElorriaga/datachallenge-test/blob/master/data/Q9Y261.xml'])
    print("File downloaded")
# Define the PythonOperator to process the UniProt XML file and store the data in Neo4j
def process_uniprot_xml():
    # Connect to the Neo4j graph database
    #graph = Graph(host=neo4j_conn.host, user=neo4j_conn.login, password=neo4j_conn.password)
    graph  = Graph("neo4j://127.0.0.1:7687", auth=("neo4j", "user_password"))
    print("Connection to graph database successful")

    # Open the XML file
    with open('/opt/airflow/dags/Q9Y261.xml', 'r') as f:
        print("file found")
        xml_str = f.read()
        print("file read")
    # Parse the XML using minidom
    dom = minidom.parseString(xml_str)
    print("file parsed")
    # Get the root element
    root = dom.documentElement
    print("file root")

    # Iterate over the entry elements and create nodes and relationships
    entries = root.getElementsByTagName('entry')
    print(f"entries done: {entries}") 
    for entry in entries:
        # Extract information from the entry
        protein   = entry.getElementsByTagName('protein')[0]
        print(f"done with protein: {protein}") 
        gene      = entry.getElementsByTagName('gene')[0]
        print(f"done with gene: {gene}")
        organism  = entry.getElementsByTagName('organism')[0]
        print(f"done with organism: {organism}")
        reference = entry.getElementsByTagName('reference')[0]
        print(f"done with reference: {reference}")

        # Create nodes for the protein, gene, organism, and reference
        protein_node   = Node('Protein', name=protein.getElementsByTagName('fullName')[0].firstChild.nodeValue)
        print(f"Node created for protein: {protein_node}") 
        gene_node      = Node('Gene', name=gene.getAttribute('name'))
        print(f"Node created for gene: {gene_node}") 
        organism_node  = Node('Organism', name=organism.getElementsByTagName('name')[0].firstChild.nodeValue)
        print(f"Node created for organism: {organism_node}")  
        reference_node = Node('Reference', key=reference.getAttribute('key'))
        print(f"Node created for reference: {reference_node}")  

        # Create relationships between the nodes
        protein_gene_rel      = Relationship(protein_node, 'FROM_GENE', gene_node)
        print(f"relationship created for protein_gene_rel: {protein_gene_rel}") 
        gene_organism_rel     = Relationship(protein_node, 'IN_ORGANISM', organism_node)
        print(f"relationship created for gene_organism_rel: {gene_organism_rel}") 
        protein_reference_rel = Relationship(protein_node, 'HAS_REFERENCE', reference_node)
        print(f"relationship created for protein_reference_rel: {protein_reference_rel}") 

        # Add the nodes and relationships to the database
        graph.create(protein_node)
        print(f"graph added to model for protein_node: {protein_node}") 
        graph.create(gene_node)
        print(f"graph added to model for gene_node: {gene_node}") 
        graph.create(organism_node)
        print(f"graph added to model for organism_node: {organism_node}") 
        graph.create(reference_node)
        print(f"graph added to model for reference_node: {reference_node}") 
        graph.create(protein_gene_rel)
        print(f"graph added to model for protein_gene_rel: {protein_gene_rel}") 
        graph.create(gene_organism_rel)
        print(f"graph added to model for gene_organism_rel: {gene_organism_rel}") 
        graph.create(protein_reference_rel)
        print(f"graph added to model for protein_reference_rel: {protein_reference_rel}")  

# Define the PythonOperator to query the Neo4j graph database
def query_neo4j():
    # Connect to the Neo4j graph database
    graph  = Graph("neo4j://127.0.0.1:7687", auth=("neo4j", "user_password"))

    # Define a Cypher query to retrieve the number of Protein nodes in the graph
    cypher_query = 'MATCH (p:Protein) RETURN count(p)'

    # Execute the query using the Neo4j driver and print the result
    result = graph.run(cypher_query).evaluate()
    print(f'Number of Protein nodes: {result}')

with DAG('uniprot_neo4j_pipeline', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    # Define the BashOperator to download the UniProt XML file
    download_op = PythonOperator(
        task_id         = 'download_file',
        python_callable = download_uniprot_xml,
    )
    
    #download_op = BashOperator(
    #    task_id      = 'download_uniprot_xml',
    #    #bash_command = 'curl -o uniprot.xml https://www.uniprot.org/uniprot/?query=*&format=xml', #TODO change for url github
    #    bash_command = 'curl -o /tmp/Q9Y261.xml https://github.com/JuanMartinElorriaga/datachallenge-test/blob/master/data/Q9Y261.xml && pwd'
    #)

    # Define the PythonOperator to process the UniProt XML file
    process_op = PythonOperator(
        task_id         = 'process_uniprot_xml',
        python_callable = process_uniprot_xml
    )

    # Define the Neo4jOperator to create an index on the Protein nodes
    create_index_op = Neo4jOperator(
        task_id       = 'create_index',
        sql           = 'CREATE INDEX ON :Protein(accession)',
        neo4j_conn_id = 'neo4j_default'
        #uri          = environ.get('NEO4J_URI', 'bolt://localhost:7687'),
        #auth         = (environ.get('NEO4J_USER', 'neo4j'), environ.get('NEO4J_PASSWORD', 'neo4j'))
    )

    # Define the PythonOperator to query the Neo4j graph database
    query_op = PythonOperator(
        task_id         = 'query_neo4j',
        python_callable = query_neo4j
    )

    # Set the dependencies between the tasks
    download_op >> process_op >> create_index_op >> query_op