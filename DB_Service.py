import psycopg2
from psycopg2 import extras

class PostgresClient:
    def __init__(self, config):
        self.config = config

    def execute_query(self, sql):
        """Standard execution with RealDictCursor for JSON-friendly results."""
        conn = None
        try:
            conn = psycopg2.connect(**self.config, sslmode='require')
            cur = conn.cursor(cursor_factory=extras.RealDictCursor)
            cur.execute(sql)
            
            if cur.description:
                res = cur.fetchall()
            else:
                conn.commit()
                res = [{"message": "Success"}]
                
            cur.close()
            return res
        except Exception as e:
            return {"error": str(e)}
        finally:
            if conn:
                conn.close()

    def get_query_metadata(self, user_query):
        """
        DYNAMIC METADATA RESOLUTION:
        Interrogates the database to find real columns for 100+ tables.
        Prevents hallucinations by fetching the live schema.
        """
        clean_sql = user_query.strip().rstrip(';')
        describe_wrapper = f"SELECT * FROM ({clean_sql}) AS virtual_query WHERE 1=0"
        
        try:
            conn = psycopg2.connect(**self.config, sslmode='require')
            cur = conn.cursor()
            cur.execute(describe_wrapper)
            colnames = [desc[0] for desc in cur.description]
            cur.close()
            conn.close()
            return colnames
        except Exception as e:
            return []

    def investigate(self, sql):
        """
        FULL-SPECTRUM INVESTIGATION:
        Analyzes Execution Plans, I/O (RAM vs Disk), and Catalog Indexes.
        """
        clean_sql = sql.strip().rstrip(';')
        wrapped_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {clean_sql}"
        
        raw_output = self.execute_query(wrapped_query)
        
        # --- INFRASTRUCTURE AUDIT ---
        # Checks the system catalog to see what optimizations are already live.
        idx_query = """
            SELECT indexname FROM pg_indexes 
            WHERE tablename IN ('customers', 'orders', 'payments', 'transactions');
        """
        index_check = self.execute_query(idx_query)
        existing_indexes = [row['indexname'] for row in index_check] if isinstance(index_check, list) else []

        if isinstance(raw_output, dict) and "error" in raw_output:
            return {**raw_output, "existing_indexes": existing_indexes}

        try:
            plan_root = raw_output[0]['QUERY PLAN'][0]
            plan_details = plan_root.get('Plan', {})
            
            # Detects "Sequential Scans" which indicate a lack of optimization.
            return {
                "exec_time": plan_root.get('Execution Time', 0),
                "hits": plan_details.get('Shared Hit Blocks', 0),   # Data from RAM
                "reads": plan_details.get('Shared Read Blocks', 0), # Data from Disk
                "is_scan": "Seq Scan" in str(plan_details),
                "total_cost": plan_details.get('Total Cost', 0),
                "existing_indexes": existing_indexes 
            }
        except (KeyError, IndexError):
            return {"error": "Plan parsing failed", "existing_indexes": existing_indexes}
