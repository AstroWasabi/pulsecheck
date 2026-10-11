"""                                                                                          
tests/test_db.py                                                                             
"""                                                                                          
                                                                                                
import pytest                                                                                
import pytest_asyncio                                                                        
import os                                                                                    
from pulsecheck.db import Database                                                           
                                                                                                
TEST_DB = "test_pulsecheck.db"                                                               
                                                                                                
                                                                                                
@pytest_asyncio.fixture                                                                      
async def db():                                                                              
    if os.path.exists(TEST_DB):                                                              
        os.remove(TEST_DB)                                                                   
    database = Database(db_path=TEST_DB)                                                     
    await database.init_db()                                                                 
    yield database                                                                           
    await database.close()                                                                   
    if os.path.exists(TEST_DB):                                                              
        os.remove(TEST_DB)                                                                   
                                                                                                
                                                                                                
@pytest.mark.asyncio                                                                         
async def test_monitor_crud(db):                                                             
    # 1. Create monitor                                                                      
    m_id = await db.create_monitor(name="Google", url="https://google.com", interval=15)     
    assert m_id == 1                                                                         
                                                                                                
    # 2. Get monitor                                                                         
    m = await db.get_monitor(m_id)                                                           
    assert m["name"] == "Google"                                                             
    assert m["url"] == "https://google.com"                                                  
    assert m["interval_seconds"] == 15                                                       
    assert m["is_active"] == 1                                                               
                                                                                                
    # 3. Toggle monitor                                                                      
    new_state = await db.toggle_monitor(m_id)                                                
    assert new_state == 0                                                                    
    m_toggled = await db.get_monitor(m_id)                                                   
    assert m_toggled["is_active"] == 0                                                       
                                                                                                
    # 4. Delete monitor                                                                      
    deleted = await db.delete_monitor(m_id)                                                  
    assert deleted is True                                                                   
    assert await db.get_monitor(m_id) is None                                                
                                                                                                
                                                                                                
@pytest.mark.asyncio                                                                         
async def test_check_results_and_stats(db):                                                  
    m_id = await db.create_monitor(name="API", url="https://api.test", interval=30)          
                                                                                                
    # Record 3 checks: 2 up, 1 down                                                          
    await db.record_result(monitor_id=m_id, status_code=200, latency_ms=40.0, is_up=True)    
    await db.record_result(monitor_id=m_id, status_code=200, latency_ms=60.0, is_up=True)    
    await db.record_result(monitor_id=m_id, status_code=500, latency_ms=100.0, is_up=False,  
error="Server Error")                                                                          
                                                                                                
    stats = await db.get_monitor_stats(m_id)                                                 
    assert stats["total_checks"] == 3                                                        
    assert stats["uptime_percentage"] == pytest.approx(66.67, rel=1e-2)                      
    assert stats["avg_latency_ms"] == pytest.approx(50.0, rel=1e-2)                          
    assert stats["current_status"] == "down"