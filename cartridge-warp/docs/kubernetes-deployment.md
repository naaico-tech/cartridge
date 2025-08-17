# Cartridge-Warp Kubernetes Deployment Architecture

This document provides comprehensive diagrams and examples for deploying cartridge-warp in Kubernetes clusters with different architectural patterns and scaling strategies.

## 1. Kubernetes Deployment Architecture Overview

```mermaid
graph TB
    %% External Systems
    subgraph "External Data Sources"
        MongoDB[(MongoDB Cluster)]
        MySQL[(MySQL Cluster)]  
        BigQuery[(BigQuery)]
    end

    %% Kubernetes Cluster
    subgraph "Kubernetes Cluster"
        %% Ingress Layer
        subgraph "Ingress Layer"
            Ingress[Ingress Controller<br/>- SSL Termination<br/>- Load Balancing<br/>- Routing]
        end

        %% Application Layer
        subgraph "Application Namespace: cartridge-warp"
            %% Single Schema Deployments
            subgraph "Single Schema Deployments"
                EcomDeploy[Ecommerce CDC<br/>Deployment<br/>- 3 replicas<br/>- MongoDB → PostgreSQL<br/>- Resource: 2 CPU, 4Gi RAM]
                
                UsersDeploy[Users CDC<br/>Deployment<br/>- 2 replicas<br/>- MySQL → PostgreSQL<br/>- Resource: 1 CPU, 2Gi RAM]
                
                AnalyticsDeploy[Analytics CDC<br/>Deployment<br/>- 1 replica<br/>- BigQuery → PostgreSQL<br/>- Resource: 1 CPU, 2Gi RAM]
            end

            %% Multi-Schema Deployment (Development)
            subgraph "Multi-Schema Deployment"
                DevDeploy[Development CDC<br/>Deployment<br/>- 1 replica<br/>- All schemas<br/>- Resource: 4 CPU, 8Gi RAM]
            end

            %% Services
            subgraph "Services"
                EcomSvc[Ecommerce Service<br/>- ClusterIP<br/>- Port 8080<br/>- Metrics endpoint]
                
                UsersSvc[Users Service<br/>- ClusterIP<br/>- Port 8080<br/>- Health checks]
                
                AnalyticsSvc[Analytics Service<br/>- ClusterIP<br/>- Port 8080<br/>- Status endpoint]
                
                DevSvc[Development Service<br/>- NodePort<br/>- Port 30080<br/>- Debug access]
            end

            %% Configuration
            subgraph "Configuration Management"
                ConfigMap[ConfigMaps<br/>- Base configurations<br/>- Environment settings<br/>- Table definitions]
                
                Secrets[Secrets<br/>- Database credentials<br/>- API keys<br/>- Certificates]
                
                PVC[Persistent Volume Claims<br/>- Log storage<br/>- Temporary data<br/>- Configuration backups]
            end
        end

        %% Infrastructure Layer  
        subgraph "Infrastructure Namespace: cartridge-infra"
            %% Monitoring Stack
            subgraph "Monitoring Stack"
                Prometheus[Prometheus<br/>- Metrics collection<br/>- Alert rules<br/>- Service discovery]
                
                Grafana[Grafana<br/>- Dashboards<br/>- Visualization<br/>- Alerting]
                
                AlertManager[Alert Manager<br/>- Alert routing<br/>- Notifications<br/>- Escalation]
            end

            %% Logging Stack
            subgraph "Logging Stack"
                Fluentd[Fluentd<br/>- Log collection<br/>- Log parsing<br/>- Log forwarding]
                
                ElasticSearch[Elasticsearch<br/>- Log storage<br/>- Log indexing<br/>- Search API]
                
                Kibana[Kibana<br/>- Log visualization<br/>- Log analysis<br/>- Dashboard]
            end

            %% Database Layer
            subgraph "Database Layer"
                PostgresOperator[PostgreSQL Operator<br/>- Database management<br/>- Backup automation<br/>- High availability]
                
                PostgresCluster[(PostgreSQL Cluster<br/>- Primary + Replica<br/>- Connection pooling<br/>- Automatic failover)]
                
                Redis[(Redis Cluster<br/>- Caching layer<br/>- Session storage<br/>- Rate limiting)]
            end
        end

        %% Storage Layer
        subgraph "Storage Layer"
            StorageClass[Storage Classes<br/>- SSD for databases<br/>- HDD for logs<br/>- Network storage]
            
            Volumes[Persistent Volumes<br/>- Database storage<br/>- Log storage<br/>- Configuration storage]
        end

        %% Network Policies
        subgraph "Network Security"
            NetworkPol[Network Policies<br/>- Namespace isolation<br/>- Service communication<br/>- External access rules]
        end
    end

    %% External Destinations
    subgraph "External Destinations"
        PostgreSQLExt[(External PostgreSQL)]
        BigQueryExt[(BigQuery Destination)]
        DataWarehouse[(Data Warehouse)]
    end

    %% Connections
    MongoDB --> EcomDeploy
    MySQL --> UsersDeploy  
    BigQuery --> AnalyticsDeploy
    
    EcomDeploy --> EcomSvc
    UsersDeploy --> UsersSvc
    AnalyticsDeploy --> AnalyticsSvc
    DevDeploy --> DevSvc
    
    ConfigMap --> EcomDeploy
    ConfigMap --> UsersDeploy
    ConfigMap --> AnalyticsDeploy
    ConfigMap --> DevDeploy
    
    Secrets --> EcomDeploy
    Secrets --> UsersDeploy
    Secrets --> AnalyticsDeploy
    
    EcomDeploy --> PostgresCluster
    UsersDeploy --> PostgresCluster
    AnalyticsDeploy --> PostgresCluster
    
    EcomDeploy --> PostgreSQLExt
    UsersDeploy --> PostgreSQLExt
    AnalyticsDeploy --> DataWarehouse
    
    EcomSvc --> Prometheus
    UsersSvc --> Prometheus
    AnalyticsSvc --> Prometheus
    
    Prometheus --> Grafana
    Prometheus --> AlertManager
    
    EcomDeploy --> Fluentd
    UsersDeploy --> Fluentd
    AnalyticsDeploy --> Fluentd
    Fluentd --> ElasticSearch
    ElasticSearch --> Kibana
    
    Ingress --> EcomSvc
    Ingress --> UsersSvc
    Ingress --> AnalyticsSvc

    %% Styling
    classDef external fill:#e3f2fd
    classDef deployment fill:#e8f5e8
    classDef service fill:#fff3e0
    classDef config fill:#fce4ec
    classDef monitoring fill:#f3e5f5
    classDef storage fill:#e0f2f1
    classDef network fill:#fff8e1

    class MongoDB,MySQL,BigQuery,PostgreSQLExt,BigQueryExt,DataWarehouse external
    class EcomDeploy,UsersDeploy,AnalyticsDeploy,DevDeploy deployment
    class EcomSvc,UsersSvc,AnalyticsSvc,DevSvc service
    class ConfigMap,Secrets,PVC config
    class Prometheus,Grafana,AlertManager,Fluentd,ElasticSearch,Kibana monitoring
    class StorageClass,Volumes,PostgresCluster,Redis storage
    class NetworkPol,Ingress network
```

## 2. Single Schema Deployment Pattern (Production)

```mermaid
graph LR
    %% Source Database
    MongoDB[(MongoDB<br/>ecommerce_db)]

    %% Kubernetes Deployment
    subgraph "Kubernetes Cluster"
        subgraph "cartridge-warp-ecommerce Deployment"
            subgraph "Pod Replicas"
                Pod1[Pod 1<br/>cartridge-warp-ecommerce<br/>- Schema: ecommerce<br/>- Tables: users, orders<br/>- CPU: 2, Memory: 4Gi]
                
                Pod2[Pod 2<br/>cartridge-warp-ecommerce<br/>- Schema: ecommerce<br/>- Tables: products, reviews<br/>- CPU: 2, Memory: 4Gi]
                
                Pod3[Pod 3<br/>cartridge-warp-ecommerce<br/>- Schema: ecommerce<br/>- Tables: categories, inventory<br/>- CPU: 2, Memory: 4Gi]
            end
            
            subgraph "Load Balancer Service"
                LB[Service: cartridge-warp-ecommerce<br/>- Type: ClusterIP<br/>- Port: 8080<br/>- Selector: app=cartridge-warp-ecommerce]
            end
            
            subgraph "Configuration"
                CM[ConfigMap<br/>- Base configuration<br/>- Table assignments<br/>- Performance tuning]
                
                Secret[Secret<br/>- MongoDB credentials<br/>- PostgreSQL credentials<br/>- SSL certificates]
            end
            
            subgraph "Storage"
                PVC1[PVC: logs-storage<br/>- Size: 10Gi<br/>- StorageClass: fast-ssd]
                
                PVC2[PVC: temp-storage<br/>- Size: 5Gi<br/>- StorageClass: standard]
            end
        end
        
        subgraph "Monitoring"
            ServiceMonitor[ServiceMonitor<br/>- Scrape interval: 30s<br/>- Metrics endpoint: /metrics<br/>- Labels: cartridge-warp]
            
            PodMonitor[PodMonitor<br/>- Pod metrics collection<br/>- Resource monitoring<br/>- Health checks]
        end
    end

    %% Destination Database
    PostgreSQL[(PostgreSQL<br/>warehouse_db)]

    %% Connections
    MongoDB --> Pod1
    MongoDB --> Pod2
    MongoDB --> Pod3
    
    Pod1 --> LB
    Pod2 --> LB
    Pod3 --> LB
    
    CM --> Pod1
    CM --> Pod2
    CM --> Pod3
    
    Secret --> Pod1
    Secret --> Pod2
    Secret --> Pod3
    
    Pod1 --> PostgreSQL
    Pod2 --> PostgreSQL
    Pod3 --> PostgreSQL
    
    Pod1 --> ServiceMonitor
    Pod2 --> ServiceMonitor
    Pod3 --> ServiceMonitor

    %% Styling
    classDef database fill:#e1f5fe
    classDef pod fill:#e8f5e8
    classDef service fill:#fff3e0
    classDef config fill:#fce4ec
    classDef storage fill:#e0f2f1
    classDef monitoring fill:#f3e5f5

    class MongoDB,PostgreSQL database
    class Pod1,Pod2,Pod3 pod
    class LB service
    class CM,Secret config
    class PVC1,PVC2 storage
    class ServiceMonitor,PodMonitor monitoring
```

## 3. Complete Kubernetes Manifests

### Deployment Configuration

```yaml
# cartridge-warp-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cartridge-warp-ecommerce
  namespace: cartridge-warp
  labels:
    app: cartridge-warp
    schema: ecommerce
    component: cdc-processor
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
      maxSurge: 1
  selector:
    matchLabels:
      app: cartridge-warp
      schema: ecommerce
  template:
    metadata:
      labels:
        app: cartridge-warp
        schema: ecommerce
        component: cdc-processor
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: cartridge-warp
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
      - name: cartridge-warp
        image: cartridge/warp:v1.0.0
        imagePullPolicy: Always
        command: ["cartridge-warp"]
        args: 
        - "run"
        - "--mode=single"
        - "--schema=$(SCHEMA_NAME)"
        - "--config=/etc/cartridge-warp/config.yaml"
        env:
        - name: SCHEMA_NAME
          value: "ecommerce"
        - name: CARTRIDGE_WARP_SOURCE__CONNECTION_STRING
          valueFrom:
            secretKeyRef:
              name: cartridge-warp-secrets
              key: mongodb-connection-string
        - name: CARTRIDGE_WARP_DESTINATION__CONNECTION_STRING
          valueFrom:
            secretKeyRef:
              name: cartridge-warp-secrets
              key: postgresql-connection-string
        - name: PROMETHEUS_METRICS_ENABLED
          value: "true"
        - name: PROMETHEUS_METRICS_PORT
          value: "8080"
        - name: LOG_LEVEL
          value: "INFO"
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        ports:
        - name: metrics
          containerPort: 8080
          protocol: TCP
        - name: health
          containerPort: 8081
          protocol: TCP
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
          limits:
            cpu: "2"
            memory: "4Gi"
        volumeMounts:
        - name: config-volume
          mountPath: /etc/cartridge-warp
          readOnly: true
        - name: logs-volume
          mountPath: /var/log/cartridge-warp
        - name: temp-volume
          mountPath: /tmp/cartridge-warp
        livenessProbe:
          httpGet:
            path: /health
            port: health
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /ready
            port: health
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 1
        startupProbe:
          httpGet:
            path: /startup
            port: health
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 6
      volumes:
      - name: config-volume
        configMap:
          name: cartridge-warp-config
          defaultMode: 0644
      - name: logs-volume
        persistentVolumeClaim:
          claimName: cartridge-warp-logs
      - name: temp-volume
        emptyDir:
          sizeLimit: 1Gi
      nodeSelector:
        workload-type: compute-intensive
      tolerations:
      - key: "cartridge-workload"
        operator: "Equal"
        value: "true"
        effect: "NoSchedule"
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchExpressions:
                - key: app
                  operator: In
                  values: ["cartridge-warp"]
              topologyKey: kubernetes.io/hostname
```

### Service Configuration

```yaml
# cartridge-warp-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: cartridge-warp-ecommerce
  namespace: cartridge-warp
  labels:
    app: cartridge-warp
    schema: ecommerce
  annotations:
    service.alpha.kubernetes.io/tolerate-unready-endpoints: "true"
spec:
  type: ClusterIP
  ports:
  - name: metrics
    port: 8080
    targetPort: metrics
    protocol: TCP
  - name: health
    port: 8081
    targetPort: health
    protocol: TCP
  selector:
    app: cartridge-warp
    schema: ecommerce

---
apiVersion: v1
kind: Service
metadata:
  name: cartridge-warp-ecommerce-headless
  namespace: cartridge-warp
  labels:
    app: cartridge-warp
    schema: ecommerce
  annotations:
    service.alpha.kubernetes.io/tolerate-unready-endpoints: "true"
spec:
  clusterIP: None
  ports:
  - name: metrics
    port: 8080
    targetPort: metrics
  selector:
    app: cartridge-warp
    schema: ecommerce
```

### Configuration Management

```yaml
# cartridge-warp-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cartridge-warp-config
  namespace: cartridge-warp
data:
  config.yaml: |
    mode: "single"
    single_schema_name: "ecommerce"
    
    # Performance Configuration
    performance:
      stream_batch_size: 1000
      write_batch_size: 500
      polling_interval: 5
      connection_timeout: 30
      read_timeout: 60
      max_retries: 3
      retry_delay: 5
    
    # Source Configuration Template (credentials from env)
    source:
      type: "mongodb"
      database: "ecommerce_db"
      change_detection_strategy: "change_stream"
      timezone: "UTC"
      replica_set: "rs0"
      auth_source: "admin"
      ssl_enabled: true
      read_preference: "secondaryPreferred"
      connection_pool_size: 10
      connection_pool_max_idle_time: 300
    
    # Destination Configuration Template (credentials from env)
    destination:
      type: "postgresql"
      schema: "ecommerce"
      pool_size: 10
      max_overflow: 20
      batch_size: 1000
      transaction_timeout: 300
      connection_timeout: 30
      command_timeout: 60
    
    # Schema Configuration
    schemas:
    - name: "ecommerce"
      table_whitelist: ["users", "orders", "products", "categories", "inventory", "reviews"]
      max_parallel_streams: 3
      
      tables:
        users:
          batch_size: 500
          polling_interval: 10
          full_load_enabled: true
          parallel_streams: 1
        orders:
          batch_size: 1000
          polling_interval: 5
          full_load_enabled: true
          parallel_streams: 2
        products:
          batch_size: 2000
          polling_interval: 15
          full_load_enabled: false
          parallel_streams: 1
        categories:
          batch_size: 1000
          polling_interval: 30
          full_load_enabled: true
          parallel_streams: 1
        inventory:
          batch_size: 1500
          polling_interval: 10
          full_load_enabled: true
          parallel_streams: 1
        reviews:
          batch_size: 800
          polling_interval: 20
          full_load_enabled: false
          parallel_streams: 1
    
    # Monitoring Configuration
    monitoring:
      metrics_enabled: true
      metrics_port: 8080
      health_check_enabled: true
      health_check_port: 8081
      prometheus_endpoint: "/metrics"
      health_endpoint: "/health"
      readiness_endpoint: "/ready"
      startup_endpoint: "/startup"
      
    # Logging Configuration
    logging:
      level: "INFO"
      format: "structured"
      correlation_id_enabled: true
      sensitive_data_masking: true
      log_file: "/var/log/cartridge-warp/cartridge-warp.log"
      max_file_size: "100MB"
      max_files: 10

---
apiVersion: v1
kind: Secret
metadata:
  name: cartridge-warp-secrets
  namespace: cartridge-warp
type: Opaque
stringData:
  mongodb-connection-string: "mongodb://admin:password@mongodb.example.com:27017/ecommerce_db?ssl=true&authSource=admin&replicaSet=rs0"
  postgresql-connection-string: "postgresql://warp_user:password@postgres.example.com:5432/warehouse?sslmode=require&connect_timeout=30"
```

### Persistent Storage

```yaml
# cartridge-warp-pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: cartridge-warp-logs
  namespace: cartridge-warp
  labels:
    app: cartridge-warp
    component: storage
spec:
  accessModes:
  - ReadWriteOnce
  storageClassName: fast-ssd
  resources:
    requests:
      storage: 10Gi

---
apiVersion: v1
kind: StorageClass
metadata:
  name: fast-ssd
provisioner: kubernetes.io/aws-ebs
parameters:
  type: gp3
  fsType: ext4
  encrypted: "true"
allowVolumeExpansion: true
reclaimPolicy: Retain
volumeBindingMode: WaitForFirstConsumer
```

### Monitoring Configuration

```yaml
# cartridge-warp-monitoring.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: cartridge-warp-ecommerce
  namespace: cartridge-warp
  labels:
    app: cartridge-warp
    schema: ecommerce
spec:
  selector:
    matchLabels:
      app: cartridge-warp
      schema: ecommerce
  endpoints:
  - port: metrics
    interval: 30s
    path: /metrics
    honorLabels: true
    scrapeTimeout: 10s
    metricRelabelings:
    - sourceLabels: [__name__]
      regex: 'cartridge_warp_.*'
      action: keep

---
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: cartridge-warp-pods
  namespace: cartridge-warp
  labels:
    app: cartridge-warp
spec:
  selector:
    matchLabels:
      app: cartridge-warp
  podMetricsEndpoints:
  - port: metrics
    interval: 30s
    path: /metrics

---
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: cartridge-warp-alerts
  namespace: cartridge-warp
  labels:
    app: cartridge-warp
spec:
  groups:
  - name: cartridge-warp
    rules:
    - alert: CartridgeWarpDown
      expr: up{job="cartridge-warp-ecommerce"} == 0
      for: 1m
      labels:
        severity: critical
      annotations:
        summary: "Cartridge-Warp service is down"
        description: "Cartridge-Warp {{ $labels.instance }} has been down for more than 1 minute."
    
    - alert: CartridgeWarpHighErrorRate
      expr: rate(cartridge_warp_errors_total[5m]) > 0.1
      for: 2m
      labels:
        severity: warning
      annotations:
        summary: "High error rate in Cartridge-Warp"
        description: "Cartridge-Warp {{ $labels.instance }} error rate is {{ $value }} errors per second."
    
    - alert: CartridgeWarpReplicationLag
      expr: cartridge_warp_replication_lag_seconds > 300
      for: 5m
      labels:
        severity: warning
      annotations:
        summary: "High replication lag in Cartridge-Warp"
        description: "Cartridge-Warp {{ $labels.instance }} replication lag is {{ $value }} seconds."
```

## 4. Deployment Strategies

### Rolling Update Strategy
```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxUnavailable: 1
    maxSurge: 1
```

### Blue-Green Deployment
```yaml
# Blue deployment
metadata:
  name: cartridge-warp-ecommerce-blue
  labels:
    version: blue
    
# Green deployment  
metadata:
  name: cartridge-warp-ecommerce-green
  labels:
    version: green

# Service selector switch
spec:
  selector:
    app: cartridge-warp
    schema: ecommerce
    version: blue  # Switch to green for deployment
```

### Canary Deployment
```yaml
# Main deployment (90% traffic)
spec:
  replicas: 9
  
# Canary deployment (10% traffic)
spec:
  replicas: 1
  template:
    metadata:
      labels:
        app: cartridge-warp
        version: canary
```

## Key Kubernetes Deployment Features

### 1. **Resource Management**
- **CPU and Memory Limits**: Prevent resource exhaustion
- **Quality of Service**: Guaranteed, Burstable, BestEffort classes
- **Node Affinity**: Optimize placement based on node characteristics

### 2. **High Availability**
- **Pod Anti-Affinity**: Distribute replicas across nodes
- **Rolling Updates**: Zero-downtime deployments
- **Health Checks**: Automatic restart of failed containers

### 3. **Security**
- **Network Policies**: Control pod-to-pod communication
- **RBAC**: Role-based access control for service accounts
- **Pod Security Context**: Run as non-root user with limited privileges

### 4. **Observability**
- **Prometheus Integration**: Comprehensive metrics collection
- **Structured Logging**: Centralized log aggregation
- **Health Endpoints**: Liveness, readiness, and startup probes

### 5. **Configuration Management**
- **ConfigMaps**: Environment-specific configuration
- **Secrets**: Secure credential management
- **Environment Variables**: Runtime configuration overrides

This Kubernetes deployment architecture provides a production-ready, scalable, and maintainable solution for running cartridge-warp CDC operations in container orchestration environments.
