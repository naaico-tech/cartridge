# Cartridge-Warp Documentation

Welcome to the comprehensive documentation for cartridge-warp, a modular Change Data Capture (CDC) streaming platform for real-time and batch data synchronization.

## 🎯 Getting Started

- **[README](../README.md)** - Quick start guide and basic usage
- **[PLAN](../PLAN.md)** - Overall architecture and implementation roadmap
- **[DEVELOPMENT](../DEVELOPMENT.md)** - Development environment setup

## 🏗️ Architecture & Design

### Visual Architecture Guides
- **[Architecture Diagrams](./architecture-diagrams.md)** - Modular design flow and data processing diagrams
- **[Connection Flow Example](./connection-flow-example.md)** - End-to-end MongoDB → PostgreSQL CDC example
- **[Kubernetes Deployment](./kubernetes-deployment.md)** - Production-ready container orchestration patterns

### Core Framework
- **[Core Framework](./core-framework.md)** - Foundational architecture and interfaces
- **[Metadata Management System](./metadata-management-system.md)** - Position tracking, schema registry, and monitoring

## 🔧 Configuration & Features

- **[Parallelism and Filtering](./parallelism-and-filtering.md)** - Table-level parallelism and filtering configuration
- **[Development Dependencies](./development-dependencies.md)** - Dependency management and installation guide

## 🔌 Database Connectors

### Source Connectors
- **[MongoDB Connector](./mongodb-connector.md)** - Change streams, schema inference, and configuration

### Destination Connectors  
- **[PostgreSQL Destination Connector](./postgresql-destination-connector.md)** - Batch writes, schema evolution, and optimization

## 📋 Quick Reference

### Documentation Structure

```
docs/
├── README.md                           # This index
├── architecture-diagrams.md            # 🎨 Visual architecture overview
├── connection-flow-example.md          # 🔄 End-to-end connection example
├── kubernetes-deployment.md            # ☸️ K8s deployment patterns
├── core-framework.md                   # 🏗️ Foundation architecture
├── metadata-management-system.md       # 📊 Metadata and monitoring
├── parallelism-and-filtering.md        # ⚡ Performance configuration
├── development-dependencies.md         # 🛠️ Development setup
├── mongodb-connector.md                # 📥 MongoDB source connector
└── postgresql-destination-connector.md # 📤 PostgreSQL destination connector
```

### Key Concepts

- **Execution Modes**: Single schema (K8s optimized) vs. Multi-schema (development)
- **Connector Architecture**: Protocol-based source/destination interfaces
- **Schema Evolution**: Automatic detection and intelligent type conversion
- **Metadata Management**: Position tracking, schema registry, error logging
- **Monitoring**: Prometheus metrics, health checks, performance statistics

### Common Use Cases

1. **Real-time CDC**: MongoDB → PostgreSQL with change stream processing
2. **Batch Synchronization**: Incremental data updates with timestamp-based detection
3. **Schema Migration**: Automatic schema evolution with backward compatibility
4. **Multi-tenant Processing**: Independent schema processing with resource isolation
5. **Production Deployment**: Kubernetes-based scaling with comprehensive monitoring

## 🚀 Quick Navigation

| What you want to do | Where to look |
|---------------------|---------------|
| **Understand the overall design** | [Architecture Diagrams](./architecture-diagrams.md) |
| **See a complete example** | [Connection Flow Example](./connection-flow-example.md) |
| **Deploy to production** | [Kubernetes Deployment](./kubernetes-deployment.md) |
| **Configure performance** | [Parallelism and Filtering](./parallelism-and-filtering.md) |
| **Set up development** | [Development Dependencies](./development-dependencies.md) |
| **Understand connectors** | [MongoDB](./mongodb-connector.md) or [PostgreSQL](./postgresql-destination-connector.md) |
| **Track data and monitor** | [Metadata Management System](./metadata-management-system.md) |
| **Core implementation** | [Core Framework](./core-framework.md) |

## 💡 Contributing to Documentation

When adding new documentation:

1. **Follow the established structure** - Use clear headings and consistent formatting
2. **Include diagrams** - Use Mermaid syntax for visual representations
3. **Provide examples** - Include code snippets and configuration samples
4. **Update this index** - Add links to new documents in the appropriate sections
5. **Cross-reference** - Link between related documents for better navigation

### Diagram Standards

- **Mermaid syntax** for all diagrams
- **Color coding** for different component types
- **Clear labels** with descriptive text
- **Consistent styling** across all documents
- **Explanatory text** following each diagram

---

For questions or improvements to this documentation, please see our [Contributing Guidelines](../../CONTRIBUTING.md).
