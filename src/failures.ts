// SQL syntax/schema/permission and unsupported-operation errors need an operator;
// network, connection, serialization and unknown errors receive bounded retries.
export function permanentInfrastructureFailure(error: unknown) {
  return ["42501", "0A000", "42883", "42P01", "42703", "42601"].includes(
    (error as { code?: string } | null)?.code ?? "");
}
