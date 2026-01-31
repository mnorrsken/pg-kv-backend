# postkeys with CloudNativePG Example

This example demonstrates how to deploy postkeys on Kubernetes using [CloudNativePG](https://cloudnative-pg.io/) as the PostgreSQL backend.

## Overview

This setup uses [Kustomize](https://kustomize.io/) to deploy:

1. **CloudNativePG Operator** - A Kubernetes operator for managing PostgreSQL clusters
2. **PostgreSQL Cluster** - A 3-instance HA PostgreSQL cluster managed by CNPG
3. **postkeys** - The Redis-compatible server backed by PostgreSQL

## Prerequisites

- Kubernetes cluster (1.25+)
- kubectl with kustomize support (v1.14+)
- Helm 3.x (for Kustomize helm chart inflation)

## Structure

```
postkeys-with-cnpg/
├── kustomization.yaml      # Main kustomization file
├── ns.yaml                 # Namespace definitions
├── cnpg/
│   └── kustomization.yaml  # CloudNativePG operator deployment
└── postkeys/
    ├── kustomization.yaml  # postkeys helm chart deployment
    ├── cluster.yaml        # CNPG PostgreSQL cluster definition
    └── values.yaml         # Helm values for postkeys
```

## Namespaces

- `cnpg-system` - CloudNativePG operator
- `myapp` - PostgreSQL cluster and postkeys

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

3. Deploy the PostgreSQL cluster and postkeys:
   ```bash
   kubectl apply -k postkeys/
   ```

## Configuration

### PostgreSQL Cluster

The [cluster.yaml](postkeys/cluster.yaml) defines a 3-instance PostgreSQL cluster:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: postkeys-db
spec:
  instances: 3
  storage:
    size: 1Gi
```

Adjust `instances` and `storage.size` based on your requirements.

### postkeys

The [values.yaml](postkeys/values.yaml) configures postkeys:

- **replicaCount**: Number of postkeys pods (default: 2)
- **redis.password.value**: Password for Redis API authentication
- **postgresql.existingSecret**: Uses the secret created by CNPG (`postkeys-db-app`)

CNPG automatically creates a secret named `<cluster-name>-app` containing database credentials. The postkeys Helm chart is configured to use this secret.

## Verification

1. Check the PostgreSQL cluster status:
   ```bash
   kubectl get cluster -n myapp
   ```

2. Check postkeys pods:
   ```bash
   kubectl get pods -n myapp -l app.kubernetes.io/name=postkeys
   ```

3. Test the connection:
   ```bash
   kubectl port-forward svc/postkeys 6379:6379 -n myapp
   redis-cli -p 6379 -a demo-password PING
   ```

## Cleanup

```bash
kubectl delete -k .
```

## Notes

- The CloudNativePG operator must be running before creating the PostgreSQL cluster
- CNPG handles PostgreSQL high availability, failover, and backups
- postkeys connects to the read-write service endpoint managed by CNPG
- For production, configure persistent storage classes and backup destinations in the Cluster resource
