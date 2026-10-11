"""
Async Database layer for PulseCheck using aiosqlite.
Manages monitors, historical ping results, and uptime statistics.
"""

import aiosqlite
import time
from typing import Any, Dict, List, Optional

class Database:
    """
    Manages async SQLite connections and queries for PulseCheck.
    """

    def __init__(self, db_path: str = "pulsecheck.db"):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def get_connection(self) -> aiosqlite.Connection:
        """Returns active database connection with row factory enabled."""
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.db_path)
            #Row factory lets us access columns by name (dict-line) instead of tuple index
            self._conn.row_factory = aiosqlite.Row
        return self._conn

    async def init_db(self) -> None:
        """Creates tables if they dont exist"""
        conn = await self.get_connection()

        # 1. Monitors table
        await conn.execute(
            """
                CREATE TABLE IF NOT EXISTS monitors(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                interval_seconds INTEGER DEFAULT 30,
                timeout_seconds REAL DEFAULT 5.0,
                is_active BOOLEAN DEFAULT 1,
                status TEXT DEFAULT 'pending',
                created_at REAL NOT NULL
                );
            """)

        #2. Check Results table
        await conn.execute(
            """
                CREATE TABLE IF NOT EXISTS check_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                monitor_id INTEGER NOT NULL,
                timestamp REAL NOT NULL,
                status_code INTEGER DEFAULT 0,
                latency_ms REAL DEFAULT 0.0,
                is_up BOOLEAN NOT NULL,
                error_message TEXT,
                FOREIGN KEY (monitor_id) REFERENCES monitors(id) on DELETE CASCADE
                );
            """)

        await conn.commit()

    async def create_monitor(
            self,
            name:str,
            url: str,
            interval: int = 30,
            timeout: float = 5.0,
    ) -> int:
        """Inserts a new monitor and returns its ID."""
        conn = await self.get_connection()
        now = time.time()
        cursor = await conn.execute(
            """
                INSERT INTO monitors (name,url,interval_seconds,timeout_seconds,is_active,status,created_at)
                VALUES (?, ?, ?, ?, 1, 'pending', ?)
            """,
            (name,url,interval,timeout,now),
        )
        await conn.commit()
        return cursor.lastrowid

    async def get_monitor(self, monitor_id: int) -> Optional[Dict[str, Any]]:
        """Retreives a single monitor ID as a dict"""
        conn = await self.get_connection()
        cursor = await conn.execute(
            "SELECT * FROM monitors where id = ?",
            (monitor_id,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_all_monitors(self) -> List[Dict[str, Any]]:                                
            """Retrieves all monitors."""                                                        
            conn = await self.get_connection()                                                   
            cursor = await conn.execute("SELECT * FROM monitors ORDER BY id DESC")               
            rows = await cursor.fetchall()                                                       
            return [dict(r) for r in rows]

    async def get_active_monitors(self) -> List[Dict[str, Any]]:                             
            """Retrieves only active monitors (for scheduling)."""                               
            conn = await self.get_connection()                                                   
            cursor = await conn.execute("SELECT * FROM monitors WHERE is_active = 1")            
            rows = await cursor.fetchall()                                                       
            return [dict(r) for r in rows]

    async def toggle_monitor(self, monitor_id: int) -> Optional[int]:                        
            """Toggles a monitor's is_active state between 1 and 0."""                           
            conn = await self.get_connection()                                                   
            monitor = await self.get_monitor(monitor_id)                                         
            if not monitor:                                                                      
                return None                                                                      
                                                                                                 
            new_state = 0 if monitor["is_active"] else 1                                         
            await conn.execute(                                                                  
                "UPDATE monitors SET is_active = ? WHERE id = ?",                                
                (new_state, monitor_id),                                                         
            )                                                                                    
            await conn.commit()                                                                  
            return new_state                                                                     
                                                                                                 
    async def delete_monitor(self, monitor_id: int) -> bool:                                 
        """Deletes a monitor and its historical check results."""                            
        conn = await self.get_connection()                                                   
        await conn.execute("DELETE FROM check_results WHERE monitor_id = ?", (monitor_id,))  
        cursor = await conn.execute("DELETE FROM monitors WHERE id = ?", (monitor_id,))      
        await conn.commit()                                                                  
        return cursor.rowcount > 0                                                           
                                                                                                
    async def update_monitor_status(self, monitor_id: int, status: str) -> None:             
        """Updates the status column of a monitor ('up' or 'down')."""                       
        conn = await self.get_connection()                                                   
        await conn.execute(                                                                  
            "UPDATE monitors SET status = ? WHERE id = ?",                                   
            (status, monitor_id),                                                            
        )                                                                                    
        await conn.commit() 

    async def record_result(                                                                 
            self,                                                                                
            monitor_id: int,                                                                     
            status_code: int,                                                                    
            latency_ms: float,                                                                   
            is_up: bool,                                                                         
            error: Optional[str] = None,                                                         
        ) -> None:                                                                               
            """Inserts a ping check result into check_results."""                                
            conn = await self.get_connection()                                                   
            now = time.time()                                                                    
            await conn.execute(                                                                  
                """                                                                              
                INSERT INTO check_results (monitor_id, timestamp, status_code, latency_ms, is_up,
  error_message)                                                                                 
                VALUES (?, ?, ?, ?, ?, ?)                                                        
                """,                                                                             
                (monitor_id, now, status_code, latency_ms, is_up, error),                        
            )                                                                                    
            await conn.commit()                                                                  
                                                                                                 
    async def get_monitor_stats(self, monitor_id: int, limit: int = 50) -> Dict[str, Any]:   
        """                                                                                  
        Calculates aggregate metrics for a monitor:                                          
        total checks, uptime percentage, average latency, and recent history.                
        """                                                                                  
        conn = await self.get_connection()                                                   
        monitor = await self.get_monitor(monitor_id)                                         
        if not monitor:                                                                      
            return {}                                                                        
                                                                                                
        cursor = await conn.execute(                                                         
            """                                                                              
            SELECT * FROM check_results                                                      
            WHERE monitor_id = ?                                                             
            ORDER BY timestamp DESC                                                          
            LIMIT ?                                                                          
            """,                                                                             
            (monitor_id, limit),                                                             
        )                                                                                    
        rows = await cursor.fetchall()                                                       
        results = [dict(r) for r in rows]                                                    
                                                                                                
        if not results:                                                                      
            return {                                                                         
                "total_checks": 0,                                                           
                "uptime_percentage": 100.0,                                                  
                "avg_latency_ms": 0.0,                                                       
                "current_status": monitor["status"],                                         
                "recent_results": [],                                                        
            }                                                                                
                                                                                                
        total = len(results)                                                                 
        up_count = sum(1 for r in results if r["is_up"])                                     
        uptime_pct = (up_count / total) * 100.0                                              
                                                                                                
        latencies = [r["latency_ms"] for r in results if r["is_up"]]                         
        avg_lat = sum(latencies) / len(latencies) if latencies else 0.0                      
                                                                                                
        return {                                                                             
            "total_checks": total,                                                           
            "uptime_percentage": round(uptime_pct, 2),                                       
            "avg_latency_ms": round(avg_lat, 2),                                             
            "current_status": "up" if results[0]["is_up"] else "down",                       
            "recent_results": list(reversed(results)),  # Chronological order for graphs     
        }                                                                                    
                                                                                                
    async def close(self) -> None:                                                           
        """Closes the database connection cleanly."""                                        
        if self._conn is not None:                                                           
            await self._conn.close()                                                         
            self._conn = None