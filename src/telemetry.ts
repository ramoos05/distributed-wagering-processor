const values = new Map<string, number>();
export function count(name: string, value = 1) {
  values.set(name, (values.get(name) ?? 0) + value);
}
export function observe(name: string, value: number) {
  count(name + "_count");
  count(name + "_sum", value);
  values.set(name + "_max", Math.max(values.get(name + "_max") ?? 0, value));
}
export const metrics = () => Object.fromEntries(values);
export function log(event: string, context: Record<string, unknown>) {
  console.log(JSON.stringify({ timestamp: new Date().toISOString(), event,
    correlationId: null, messageId: null, transactionId: null, walletId: null, providerId: null, ...context }));
}
