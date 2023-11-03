# Distributed Inventory Management System

## Introduction

This documentation outlines the setup and functionality of a distributed inventory system designed to be fault-tolerant, highly available, and scalable, with multiple geographically distributed server processes.

## Destributed Inventory System

Implemented with a 3-server architecture for simplicity:

- **Architecture**
  - Outlined in the detailed system architecture diagram below.

<img width="814" alt="Screenshot 2023-11-03 at 23 14 17" src="https://github.com/yathindrak/DistributedInventorySystem/assets/32919513/249bac19-05ee-4830-8299-2574c7ee4c31">

  
- **Components**
  - Name service module, Distributed lock module, Server, and Client.

- **Protocols & Modules**
  - Primary-based protocol and Two-phase commit protocol for consistency.
  - gRPC for communication.
  - Etcd for dynamic service discovery.

## Assumptions

1. Maximum of 3 server instances.
2. Single product type.
3. Excludes catastrophe recovery scenarios.
4. Orders processed only if inventory matches demand.
