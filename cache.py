import sqlite3
import os
import hashlib
import time
import shutil

class FileCache:
    def __init__(self, cache_dir="/var/tmp/discohack_cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        self.db_path = os.path.join(cache_dir, "cache.db")
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                remote_path TEXT PRIMARY KEY,
                local_path TEXT,
                file_hash TEXT,
                size INTEGER,
                last_access INTEGER,
                mime_type TEXT
            )
        """)
        self.conn.commit()
    
    def get(self, remote_path):
        """Получить путь к закешированному файлу"""
        cur = self.conn.execute(
            "SELECT local_path FROM cache WHERE remote_path=?", 
            (remote_path,)
        )
        row = cur.fetchone()
        if row and os.path.exists(row[0]):
            # Обновляем время доступа
            self.conn.execute(
                "UPDATE cache SET last_access=? WHERE remote_path=?", 
                (int(time.time()), remote_path)
            )
            self.conn.commit()
            return row[0]
        return None
    
    def put(self, remote_path, cloud, mime_type=""):
        """Скачать и закешировать файл"""
        file_hash = hashlib.md5(remote_path.encode()).hexdigest()
        local_path = os.path.join(self.cache_dir, file_hash)
        
        # Скачиваем
        cloud.download_file(remote_path, local_path)
        
        size = os.path.getsize(local_path)
        
        self.conn.execute(
            """INSERT OR REPLACE INTO cache 
               (remote_path, local_path, file_hash, size, last_access, mime_type) 
               VALUES (?,?,?,?,?,?)""",
            (remote_path, local_path, file_hash, size, int(time.time()), mime_type)
        )
        self.conn.commit()
        return local_path
    
    def clear_old(self, days=7):
        """Очистить старый кеш"""
        threshold = int(time.time()) - (days * 86400)
        self.conn.execute("DELETE FROM cache WHERE last_access < ?", (threshold,))
        self.conn.commit()
    
    def is_cached(self, remote_path):
        return self.get(remote_path) is not None