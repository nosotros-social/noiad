use anyhow::Result;

use tokio_postgres::{Client, types::Oid};

#[derive(Debug, Clone)]
pub struct PostgresTable {
    pub oid: Oid,
    pub name: String,
    pub schema: String,
}

pub async fn get_publication_info(
    client: &Client,
    publication_name: &str,
) -> Result<Vec<PostgresTable>> {
    let query = "
        SELECT n.nspname, c.relname, c.oid
        FROM pg_publication_tables pt
        JOIN pg_class c ON c.relname = pt.tablename
        JOIN pg_namespace n ON n.nspname = pt.schemaname AND n.oid = c.relnamespace
        WHERE pt.pubname = $1
    ";

    let rows = client.query(query, &[&publication_name]).await?;
    let mut tables = Vec::new();
    for row in rows {
        let schema: String = row.get("nspname");
        let name: String = row.get("relname");
        let oid: u32 = row.get("oid");
        let table = PostgresTable { oid, name, schema };
        tables.push(table);
    }
    Ok(tables)
}
