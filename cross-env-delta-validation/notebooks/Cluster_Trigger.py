# %%
#Getting and Assigning Required Parameters to Start teh respective Clusters
import requests
from time import sleep

#defining prod token
dbutils.widgets.text("ProdAccessToken","")
ProdToken=dbutils.widgets.get("ProdAccessToken")

#defining Dev token
dbutils.widgets.text("DevAccessToken","")
DevToken=dbutils.widgets.get("DevAccessToken")

#defining Domain URL's
prod_domain_url="<Enter the Prod Environment URL>"
dev_domain_url="<Enter the Dev Environment URL>"

#defining Cluser ID's
Prod_Cluster_1="<Enter Cluster ID>"
Dev_Cluster_1="<Enter Cluster ID>"
Prod_Cluster_2="<Enter Cluster ID>"

# %%
#Defining Function to Start teh respective Clusters and Get its Status

def start_cluster_and_get_status(domain_url,token,cluste_id):
    """
    Start a Databricks cluster and get its status.

    Parameters:
    - domain_url (str) : Environment URL for Databricks.
    - token (str) :  Personal Access Token for Databricks
    - cluster_id(str) : ID of the cluster to start.

    Returns:
    - str: Status of the cluster.
    """

    start_cluster_url=f"{domain_url}/api/2.0/clusters/start"
    get_cluster_url=f"{domain_url}/api/2.0/clusters/get"
    headers = {
        "Authorization" : f"Bearer {pat_token}"
    }
    cluster_data = {
        "cluster_id":cluste_id
    }

    # Start the cluster
    start_response =  requests.post(start_cluster_url,headers=headers,json=cluster_data)
    
    status_response = requests.post(get_cluster_url,headers=headers,params=cluster_data)
    cluster_info=status_response.json()
    cluster_name=cluster_info["cluster_name"]

    if status_response.status_code==200:
        print(f"Cluster {cluster_name} with ID:{cluster_id} start initiated.")
    else:
        return f"Cluster {cluster_name} start failed with {status_response.text.split(',')[1]}"
    
    #Check the Status of cluster
    status_response = requests.post(get_cluster_url,headers=headers,params=cluster_data)
    cluster_info=status_response.json()

    while cluster_info['state'] == 'PENDING':
        sleep(60)
        status_response = requests.post(get_cluster_url,headers=headers,params=cluster_data)
        cluster_info=status_response.json()
        print(f"Cluster {cluster_name} status: {cluster_info['state']}")


# %%
#Starting the PROD Clusters

start_cluster_and_get_status(prod_domain_url,ProdToken,Prod_Cluster_1)
start_cluster_and_get_status(prod_domain_url,ProdToken,Prod_Cluster_2)

# %%
#Starting the DEV Clusters
start_cluster_and_get_status(dev_domain_url,DevToken,Dev_Cluster_1)


