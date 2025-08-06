# Reservo: Redis-Powered Distributed Resource Pool for God

## Efficiently manage scarce resources across cloud instances

Reservo provides a battle-tested, self-healing pool for shared resources (API tokens, GPU slots, Sessions, Job Slots) 
with Redis-backed coordination. Perfect for scaling services in cloud native environments.

## Key Features

- ✳️ **Self-healing pools** - Automatic orphan recovery
- ✳️ **Distributed Locking** - Redis-backed mutual exclusion
- ✳️ **Resource Recreation** - Expired resources are recreated based on custom logic
- ✳️ **Resource Recreation** - Expired resources are recreated based on custom logic
- ✳️ **Cloud Native (K8s) Ready** - Handles pod churn gracefully
- ✳️ **Near-zero config** - Sensible defaults + simple tuning

### Ideal For

- Fair distribution of rate limited API Tokens
- Managing slots for scarce resources like GPU/ML accelerators
- Distributed session management, login once, use session from many pods

#### Want to discuss potential issues with the library or improvement?

Just open an issue. I'm always open for collaboration. 🤝🏻

A great man once said, "If you want to go fast, go alone. But if you want to go far, go together."

