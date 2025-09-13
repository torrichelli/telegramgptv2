import { PostgresStore } from "@mastra/pg";

// Create a single shared PostgreSQL storage instance
// In production, DATABASE_URL is required. Local fallback only for development.
const connectionString = process.env.DATABASE_URL || 
  (process.env.NODE_ENV === "production" 
    ? (() => { throw new Error("DATABASE_URL is required in production"); })()
    : "postgresql://localhost:5432/mastra");

export const sharedPostgresStorage = new PostgresStore({
  connectionString,
});
