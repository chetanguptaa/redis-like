export function isStrictNumber(str: string) {
  if (typeof str !== "string" || str.trim() === "") return false;
  return !Number.isNaN(Number(str));
}
