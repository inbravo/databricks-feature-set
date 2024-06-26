#### Unity Catalog Essentials
- Databricks recommends using Unity Catalog to manage access to all data stored in cloud object storage
- If your workspace was created before November 8, 2023, it might not be enabled for Unity Catalog. An account admin must enable Unity Catalog for your workspace.
- Do not give end users storage-level access (AWS S3 etc.) to Unity Catalog managed tables or volumes. This compromises data security and governance.
- The only identity that should have access to data managed by Unity Catalog is the identity used by Unity Catalog. Ignoring this creates the following issues in your environment:
  - Access controls established in Unity Catalog can be circumvented by users who have direct access to S3 or R2 buckets.
  - Auditing, lineage, and other Unity Catalog monitoring features will not capture direct access.
  - The lifecycle of data is broken. That is, modifying, deleting, or evolving tables in Databricks will break the consumers that have direct access to storage.
- [Connect to cloud object storage using Unity Catalog](https://docs.databricks.com/en/connect/unity-catalog/index.html#connect-to-cloud-object-storage-using-unity-catalog)
- [Set up and manage Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/get-started.html#set-up-and-manage-unity-catalog)
  - Databricks on AWS supports both AWS S3 and Cloudflare R2 buckets (Public Preview) as cloud storage locations for data assets registered in Unity Catalog.
  - Cloudfare R2 is intended primarily for uses cases in which you want to avoid data egress fees, such as Delta Sharing across clouds and regions.
  - Run the SQL ```SELECT CURRENT_METASTORE()``` in the SQL query editor or a notebook. It should return a metastore ID like: ```aws-us-west-2....```
  - Grant a group the ability to create new schemas in my-catalog run this SQL:<br>
    ```GRANT CREATE SCHEMA ON my-catalog TO `data-consumers`;```
  - Create of a catalog with managed storage, followed by granting the SELECT privilege on the catalog:<br>
    ```CREATE CATALOG IF NOT EXISTS mycatalog MANAGED LOCATION 's3://depts/finance';```<br>
    ```GRANT SELECT ON mycatalog TO `finance-team`;```
- Databricks recommends against using DBFS and mounted cloud storage in Unity Catalog-enabled Databricks workspaces. [Best Practices for Unity with DBFS](https://docs.databricks.com/en/dbfs/unity-catalog.html)
  - Unity Catalog secures access to data in external locations by using full cloud URI paths to identify grants on managed object storage directories.
  - DBFS mounts use an entirely different data access model that bypasses Unity Catalog entirely.
  - Databricks recommends that you do not reuse cloud object storage volumes between DBFS mounts and UC external volumes.
- Each Unity Catalog metastore has an object storage account configured by a Databricks account administrator. Unity Catalog uses this location to store all data and metadata for Unity Catalog-managed tables.
  -  A storage account used for a Unity Catalog metastore should:
  -  Be created new for Unity Catalog.
  -  Have a custom identity policy defined for Unity Catalog.
  -  Only be accessible with Unity Catalog.
  -  Only be accessed using the identity access policies created for Unity Catalog.

