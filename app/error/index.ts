class RedisError extends Error {
  constructor(
    public code: string,
    message: string,
  ) {
    super(message);
  }
}

export default RedisError;
