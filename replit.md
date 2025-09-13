# Replit.md

## Overview

This repository contains a multi-faceted agent framework built with Mastra, combining Telegram bot functionality with advanced agent orchestration capabilities. The project integrates:

- **Telegram Bot System**: A comprehensive Python-based bot for chat activity tracking and reporting, featuring subscription/unsubscription monitoring, Excel report generation, and automated daily reporting
- **Agent Framework**: A TypeScript-based system using Mastra for creating dynamic agents with runtime context adaptation, tool selection, and multi-model support
- **Workflow Orchestration**: Inngest-powered workflow management with real-time capabilities and trigger-based automation
- **Storage & Database**: PostgreSQL storage integration with optional SQLite fallback for the Telegram bot component

The architecture supports both standalone bot operations and integrated agent workflows, making it suitable for complex automation scenarios requiring both chat interaction and intelligent agent processing.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Core Technologies
- **Frontend**: Mastra playground with real-time updates via SSE
- **Backend**: Node.js with TypeScript, Python for Telegram bot
- **Agents**: Mastra framework with dynamic configuration support
- **Workflows**: Inngest for async processing and event handling
- **Storage**: PostgreSQL primary, SQLite for bot data

### Agent System Design
The agent architecture uses dynamic configuration patterns where agents adapt their behavior based on runtime context:
- **Dynamic Instructions**: Agents modify their system prompts based on user context (subscription tier, language preferences)
- **Model Selection**: Different AI models chosen dynamically (GPT-4 for enterprise, GPT-3.5 for others)
- **Tool Access**: Context-driven tool availability (basic vs advanced analytics)
- **Multi-Provider Support**: OpenAI and OpenRouter integration for model diversity

### Telegram Bot Architecture
The bot system follows a modular approach:
- **Event Handling**: Tracks chat member updates, subscription changes
- **Database Layer**: SQLite with proper transaction management and connection pooling
- **Report Generation**: Excel file creation with pandas/openpyxl, daily automated reports
- **Scheduling**: APScheduler for time-based report delivery
- **Logging**: Comprehensive logging with rotation and Almaty timezone handling (UTC+5)

### Workflow Integration
- **Trigger System**: API route registration for external webhook handling
- **Event Processing**: Inngest functions for async workflow execution
- **Real-time Updates**: SSE for live system monitoring
- **Error Handling**: Retry mechanisms with exponential backoff

### Storage Strategy
- **Primary Storage**: PostgreSQL via DATABASE_URL for production scalability
- **Bot Storage**: Local SQLite for rapid development and simple deployment
- **File Storage**: Local filesystem for Excel reports and logs
- **State Management**: Runtime context preservation across agent interactions

### Development Environment
- **Hot Reload**: File watching with automatic restart
- **Type Safety**: Full TypeScript coverage with strict type checking
- **Testing**: pytest for Python components, structured for async testing
- **Deployment**: Replit-optimized with proper environment variable handling

The system is designed for both development flexibility and production robustness, with clear separation between the Telegram bot's Python ecosystem and the agent framework's TypeScript environment.

## External Dependencies

### AI & ML Services
- **OpenAI**: GPT models for agent responses and reasoning
- **OpenRouter**: Alternative model provider for diverse AI capabilities

### Communication Platforms
- **Telegram Bot API**: Message handling, webhook processing, chat management
- **Slack API**: Channel monitoring, message posting, user interaction

### Workflow & Infrastructure
- **Inngest**: Async workflow orchestration, event processing, real-time features
- **PostgreSQL**: Primary database for production data storage
- **Exa**: Search and information retrieval capabilities

### Data Processing
- **Pandas**: Excel report generation and data manipulation
- **OpenPyXL**: Excel file creation and formatting for bot reports
- **SQLite**: Local database for Telegram bot data storage

### Development Tools
- **Mastra Framework**: Core agent orchestration and management
- **APScheduler**: Time-based task scheduling for automated reports
- **Pino**: Structured logging for system monitoring
- **Zod**: Runtime type validation and schema definition

### Optional Integrations
- **MCP (Model Context Protocol)**: For advanced agent communication
- **LibSQL**: Alternative database option for specific use cases
- **Pytz**: Timezone handling for Almaty-based scheduling (UTC+5)

The system is architected to gracefully handle missing optional dependencies while requiring core services for full functionality.