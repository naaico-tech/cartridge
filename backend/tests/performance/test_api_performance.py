"""Performance tests for API endpoints."""

import pytest
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed


class TestAPIPerformance:
    """Test API endpoint performance."""
    
    @pytest.mark.slow
    def test_health_endpoint_performance(self, client):
        """Test health endpoint response time."""
        response_times = []
        
        # Make multiple requests to get average response time
        for _ in range(100):
            start_time = time.time()
            response = client.get("/api/v1/health")
            end_time = time.time()
            
            assert response.status_code == 200
            response_times.append(end_time - start_time)
        
        # Calculate statistics
        avg_response_time = statistics.mean(response_times)
        median_response_time = statistics.median(response_times)
        p95_response_time = statistics.quantiles(response_times, n=20)[18]  # 95th percentile
        
        # Assert performance requirements
        assert avg_response_time < 0.1  # Average should be under 100ms
        assert median_response_time < 0.05  # Median should be under 50ms
        assert p95_response_time < 0.2  # 95th percentile should be under 200ms
        
        print(f"Health endpoint performance:")
        print(f"  Average: {avg_response_time:.3f}s")
        print(f"  Median: {median_response_time:.3f}s")
        print(f"  95th percentile: {p95_response_time:.3f}s")
    
    @pytest.mark.slow
    def test_concurrent_requests_performance(self, client):
        """Test performance under concurrent load."""
        def make_request():
            start_time = time.time()
            response = client.get("/api/v1/health")
            end_time = time.time()
            return response.status_code, end_time - start_time
        
        # Test with different concurrency levels
        concurrency_levels = [1, 5, 10, 20]
        results = {}
        
        for concurrency in concurrency_levels:
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                start_time = time.time()
                
                # Submit concurrent requests
                futures = [executor.submit(make_request) for _ in range(100)]
                
                # Collect results
                response_times = []
                success_count = 0
                
                for future in as_completed(futures):
                    status_code, response_time = future.result()
                    if status_code == 200:
                        success_count += 1
                    response_times.append(response_time)
                
                total_time = time.time() - start_time
                
                results[concurrency] = {
                    'success_rate': success_count / 100,
                    'avg_response_time': statistics.mean(response_times),
                    'total_time': total_time,
                    'requests_per_second': 100 / total_time
                }
        
        # Assert performance requirements
        for concurrency, metrics in results.items():
            assert metrics['success_rate'] >= 0.95  # 95% success rate
            assert metrics['avg_response_time'] < 0.5  # Average under 500ms
            
            print(f"Concurrency {concurrency}:")
            print(f"  Success rate: {metrics['success_rate']:.2%}")
            print(f"  Avg response time: {metrics['avg_response_time']:.3f}s")
            print(f"  Requests/second: {metrics['requests_per_second']:.1f}")
    
    @pytest.mark.slow
    def test_scanner_endpoint_performance(self, client):
        """Test schema scanner endpoint performance."""
        connection_data = {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public"
        }
        
        response_times = []
        
        # Test connection endpoint performance
        for _ in range(10):  # Fewer iterations for heavier endpoints
            start_time = time.time()
            response = client.post("/api/v1/scanner/test-connection", json=connection_data)
            end_time = time.time()
            
            assert response.status_code == 200
            response_times.append(end_time - start_time)
        
        avg_response_time = statistics.mean(response_times)
        
        # Scanner endpoints can be slower due to database operations
        assert avg_response_time < 2.0  # Should be under 2 seconds
        
        print(f"Scanner test-connection performance:")
        print(f"  Average: {avg_response_time:.3f}s")
    
    @pytest.mark.slow
    def test_model_generation_performance(self, client):
        """Test model generation endpoint performance."""
        generation_request = {
            "schema_data": {
                "tables": [
                    {
                        "name": "customers",
                        "columns": [
                            {"name": "id", "type": "integer"},
                            {"name": "email", "type": "varchar"},
                            {"name": "created_at", "type": "timestamp"}
                        ]
                    },
                    {
                        "name": "orders",
                        "columns": [
                            {"name": "id", "type": "integer"},
                            {"name": "customer_id", "type": "integer"},
                            {"name": "total", "type": "decimal"}
                        ]
                    }
                ]
            },
            "model_types": ["staging", "intermediate", "marts"],
            "ai_model": "gpt-4",
            "include_tests": True,
            "include_docs": True
        }
        
        response_times = []
        
        # Test model generation performance
        for _ in range(5):  # Even fewer iterations for AI-heavy endpoints
            start_time = time.time()
            response = client.post("/api/v1/projects/generate", json=generation_request)
            end_time = time.time()
            
            assert response.status_code == 200
            response_times.append(end_time - start_time)
        
        avg_response_time = statistics.mean(response_times)
        
        # Model generation can be slow due to AI processing
        assert avg_response_time < 10.0  # Should be under 10 seconds (placeholder implementation is fast)
        
        print(f"Model generation performance:")
        print(f"  Average: {avg_response_time:.3f}s")


class TestMemoryUsage:
    """Test memory usage patterns."""
    
    @pytest.mark.slow
    def test_memory_usage_under_load(self, client):
        """Test memory usage doesn't grow excessively under load."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Make many requests
        for _ in range(1000):
            response = client.get("/api/v1/health")
            assert response.status_code == 200
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_growth = final_memory - initial_memory
        
        # Memory growth should be reasonable (less than 50MB for 1000 requests)
        assert memory_growth < 50
        
        print(f"Memory usage:")
        print(f"  Initial: {initial_memory:.1f} MB")
        print(f"  Final: {final_memory:.1f} MB")
        print(f"  Growth: {memory_growth:.1f} MB")


class TestDatabasePerformance:
    """Test database performance."""
    
    @pytest.mark.slow
    def test_database_query_performance(self, db_session):
        """Test database query performance."""
        from cartridge.models import User
        
        # Create test data
        users = []
        for i in range(100):
            user = User(
                email=f"user{i}@example.com",
                username=f"user{i}",
                full_name=f"User {i}",
                hashed_password="hashed_password",
                is_active=True
            )
            users.append(user)
        
        db_session.add_all(users)
        db_session.commit()
        
        # Test query performance
        query_times = []
        
        for _ in range(50):
            start_time = time.time()
            result = db_session.query(User).filter(User.is_active == True).all()
            end_time = time.time()
            
            assert len(result) == 100
            query_times.append(end_time - start_time)
        
        avg_query_time = statistics.mean(query_times)
        
        # Database queries should be fast
        assert avg_query_time < 0.01  # Should be under 10ms
        
        print(f"Database query performance:")
        print(f"  Average: {avg_query_time:.4f}s")
    
    @pytest.mark.slow
    def test_database_connection_pool_performance(self, db_session):
        """Test database connection pool performance."""
        from cartridge.core.database import engine
        
        # Test connection acquisition time
        acquisition_times = []
        
        for _ in range(100):
            start_time = time.time()
            conn = engine.connect()
            end_time = time.time()
            
            acquisition_times.append(end_time - start_time)
            conn.close()
        
        avg_acquisition_time = statistics.mean(acquisition_times)
        
        # Connection acquisition should be fast
        assert avg_acquisition_time < 0.01  # Should be under 10ms
        
        print(f"Connection pool performance:")
        print(f"  Average acquisition time: {avg_acquisition_time:.4f}s")