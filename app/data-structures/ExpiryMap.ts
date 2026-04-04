class ExpiryMap<K, V> extends Map<K, V> {
  private defaultTtl: number;
  private timeouts: Map<K, ReturnType<typeof setTimeout>>;

  constructor(defaultTtl: number = 0) {
    super();
    this.defaultTtl = defaultTtl;
    this.timeouts = new Map();
  }

  set(key: K, value: V, ttl: number = this.defaultTtl): this {
    if (this.timeouts.has(key)) {
      clearTimeout(this.timeouts.get(key)!);
    }
    super.set(key, value);
    if (ttl > 0) {
      const timeoutId = setTimeout(() => {
        this.delete(key);
      }, ttl);
      this.timeouts.set(key, timeoutId);
    }
    return this;
  }

  delete(key: K): boolean {
    if (this.timeouts.has(key)) {
      clearTimeout(this.timeouts.get(key)!);
      this.timeouts.delete(key);
    }
    return super.delete(key);
  }

  clear(): void {
    for (const timeoutId of this.timeouts.values()) {
      clearTimeout(timeoutId);
    }
    this.timeouts.clear();
    super.clear();
  }
}

export default ExpiryMap;
