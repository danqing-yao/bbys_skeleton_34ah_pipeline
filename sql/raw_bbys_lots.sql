CREATE OR REPLACE TABLE `barnebys-skeleton.42ah.raw_bbys_lots`
CLUSTER BY inventoryId, category_id
OPTIONS(
  description = "Deduplicated lots data combined from AWS and Azure sources, keeping latest record per inventoryId"
)
AS
SELECT
  lot_id,
  title,
  inventoryId,
  url,
  ah_id as auction_house_id,
  category_id,
  category_name,
  created,
  updated
  FROM (
    SELECT
      lot_id, title,
      CAST(inventoryId AS STRING) AS inventoryId,
      url, ah_id, category_id, category_name, created, updated
    FROM `barnebys-skeleton.42ah.raw_bbys_aws_lots`
    UNION DISTINCT
    SELECT
      lot_id, title,
      CAST(inventoryId AS STRING) AS inventoryId,
      url, ah_id, category_id, category_name, created, updated
    FROM `barnebys-skeleton.42ah.raw_bbys_azure_lots`
  );
