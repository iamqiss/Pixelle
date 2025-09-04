# Sonet Services

This directory contains the microservices for the Sonet social platform.

## New Services

### Lists Service (`list_service/`)

A Go-based microservice for managing user lists (collections of users).

**Features:**
- Create, read, update, delete user lists
- Add/remove members from lists
- List privacy controls (public/private)
- List types (custom, close_friends, family, work, school)
- Permission-based access control

**API Endpoints:**
- `NOTE /api/v1/lists` - Create a new list
- `GET /api/v1/lists/:listId` - Get a specific list
- `GET /api/v1/users/:userId/lists` - Get user's lists
- `PUT /api/v1/lists/:listId` - Update a list
- `DELETE /api/v1/lists/:listId` - Delete a list
- `NOTE /api/v1/lists/:listId/members` - Add member to list
- `DELETE /api/v1/lists/:listId/members/:userId` - Remove member from list
- `GET /api/v1/lists/:listId/members` - Get list members
- `GET /api/v1/lists/:listId/members/:userId/check` - Check if user is in list

**Database Tables:**
- `user_lists` - Main list information
- `user_list_members` - List membership relationships

**Port:** 9098

### Starterpacks Service (`starterpack_service/`)

A Go-based microservice for managing user starterpacks (collections of profiles and feeds for onboarding).

**Features:**
- Create, read, update, delete starterpacks
- Add/remove items (profiles/feeds) to starterpacks
- Starterpack privacy controls
- Item ordering within starterpacks
- Suggested starterpacks for discovery

**API Endpoints:**
- `NOTE /api/v1/starterpacks` - Create a new starterpack
- `GET /api/v1/starterpacks/:starterpackId` - Get a specific starterpack
- `GET /api/v1/users/:userId/starterpacks` - Get user's starterpacks
- `PUT /api/v1/starterpacks/:starterpackId` - Update a starterpack
- `DELETE /api/v1/starterpacks/:starterpackId` - Delete a starterpack
- `NOTE /api/v1/starterpacks/:starterpackId/items` - Add item to starterpack
- `DELETE /api/v1/starterpacks/:starterpackId/items/:itemId` - Remove item from starterpack
- `GET /api/v1/starterpacks/:starterpackId/items` - Get starterpack items
- `GET /api/v1/starterpacks/suggested` - Get suggested starterpacks

**Database Tables:**
- `starterpacks` - Main starterpack information
- `starterpack_items` - Items within starterpacks

**Port:** 9099

## Architecture

Both services follow the same architecture pattern:

```
service/
├── main.go                 # Service entry point
├── proto/                  # Protocol buffer definitions
├── models/                 # Data models and conversions
├── repository/             # Database operations
│   ├── database.go        # Connection and interface
│   └── *_repository.go    # Implementation
├── service/               # gRPC service implementation
├── go.mod                 # Go module dependencies
└── Dockerfile            # Container definition
```

## Development

### Prerequisites
- Go 1.21+
- postgresql
- Protocol Buffers compiler

### Building

```bash
# Build Lists service
cd sonet/src/services/list_service
go build -o list-service .

# Build Starterpacks service
cd sonet/src/services/starterpack_service
go build -o starterpack-service .
```

### Running

```bash
# Set environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=sonet
export DB_PASSWORD=sonet
export DB_NAME=sonet

# Run Lists service
./list-service

# Run Starterpacks service
./starterpack-service
```

### Docker

```bash
# Build and run Lists service
docker build -t sonet-list-service sonet/src/services/list_service/
docker run -p 9098:9098 sonet-list-service

# Build and run Starterpacks service
docker build -t sonet-starterpack-service sonet/src/services/starterpack_service/
docker run -p 9099:9099 sonet-starterpack-service
```

## Database Setup

Run the schema files to create the required tables:

```sql
-- For Lists service
\i sonet/database/schemas/follow_schema.sql

-- For Starterpacks service
\i sonet/database/schemas/starterpack_schema.sql
```

## Integration with Gateway

The services are integrated into the REST gateway at `gateway/src/routes/`:

- `lists.ts` - Lists API routes
- `starterpacks.ts` - Starterpacks API routes

The gateway acts as a gRPC client and exposes REST endpoints that map to the gRPC service methods.

## Migration from AT Protocol

These services provide the server-side implementation for Lists and Starterpacks functionality that was previously handled by the AT Protocol. The client-side code in `sonet-client` can be gradually migrated to use these new endpoints instead of the AT Protocol APIs.

## Testing

```bash
# Test Lists service
cd sonet/src/services/list_service
go test ./...

# Test Starterpacks service
cd sonet/src/services/starterpack_service
go test ./...
```

## Monitoring

Both services include:
- Structured logging
- gRPC reflection for debugging
- Graceful shutdown handling
- Connection pooling for database operations