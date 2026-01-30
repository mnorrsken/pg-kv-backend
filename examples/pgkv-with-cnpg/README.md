# pg-kv-backend with CloudNativePG Example

This example demonstrates how to deploy pg-kv-backend on Kubernetes using [CloudNativePG](https://cloudnative-pg.io/) as the PostgreSQL backend.

## Overview

This setup uses [Kustomize](https://kustomize.io/) to deploy:

1. **CloudNativePG Operator** - A Kubernetes operator for managing PostgreSQL clusters
2. **PostgreSQL Cluster** - A 3-instance HA PostgreSQL cluster managed by CNPG
3. **pg-kv-backend** - The Redis-compatible server backed by PostgreSQL

## Prerequisites

- Kubernetes cluster (1.25+)
- kubectl with kustomize support (v1.14+)
- Helm 3.x (for Kustomize helm chart inflation)

## Structure

```
pgkv-with-cnpg/
├── kustomization.yaml      # Main kustomization file
├── ns.yaml                 # Namespace definitions
├── cnpg/
│   └── kustomization.yaml  # CloudNativePG operator deployment
└── pgkv/
    ├── kustomization.yaml  # pg-kv-backend helm chart deployment
    ├── cluster.yaml        # CNPG PostgreSQL cluster definition
    └── values.yaml         # Helm values for pg-kv-backend
```

## Namespaces

- `cnpg-system` - CloudNativePG operator
- `myapp` - PostgreSQL cluster and pg-kv-backend

## Deployment

### Deploy everything

```bash
kubectl apply -k .
```

### Build and review manifests first

```bash
kubectl kustomize --enable-helm . | less
```

### Deploy step-by-step

1. Create namespaces and install the CNPG operator:
   ```bash
   kubectl apply -k cnpg/
   ```

2. Wait for the operator to be ready:
   ```bash
   kubectl wait --for=condition=Available deployment/cnpg-controller-manager -n cnpg-system --timeout=120s
   ```

3. Deploy the PostgreSQL cluster and pg-kv-backend:
   ```bash
   kubectl apply -k pgkv/
   ```

## Configuration

### PostgreSQL Cluster

The [cluster.yaml](pgkv/cluster.yaml) defines a 3-instance PostgreSQL cluster:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: pgkv-db
spec:
  instances: 3
  storage:
    size: 1Gi
```

Adjust `instances` and `storage.size` based on your requirements.

### pg-kv-backend

The [values.yaml](pgkv/values.yaml) configures pg-kv-backend:

- **replicaCount**: Number of pg-kv-backend pods (default: 2)
- **redis.password.value**: Password for Redis API authentication
- **postgresql.existingSecret**: Uses the secret created by CNPG (`pgkv-db-app`)

CNPG automatically creates a secret named `<cluster-name>-app` containing database credentials. The pg-kv-backend Helm chart is configured to use this secret.

## Verification

1. Check the PostgreSQL cluster status:
   ```bash
   kubectl get cluster -n myapp
   ```

2. Check pg-kv-backend pods:
   ```bash
   kubectl get pods -n myapp -l app.kubernetes.io/name=pg-kv-backend
   ```

3. Test the connection:
   ```bash
   kubectl port-forward svc/pg-kv-backend 6379:6379 -n myapp
   redis-cli -p 6379 -a demo-password PING
   ```

## Cleanup

```bash
kubectl delete -k .
```

## Notes

- The CloudNativePG operator must be running before creating the PostgreSQL cluster
- CNPG handles PostgreSQL high availability, failover, and backups
- pg-kv-backend connects to the read-write service endpoint managed by CNPG
- For production, configure persistent storage classes and backup destinations in the Cluster resource
