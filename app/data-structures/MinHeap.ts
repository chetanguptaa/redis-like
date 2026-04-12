export class MinHeap<T extends { score: number | bigint }> {
  private heap: T[] = [];

  private parent(i: number) {
    return Math.floor((i - 1) / 2);
  }
  private left(i: number) {
    return 2 * i + 1;
  }
  private right(i: number) {
    return 2 * i + 2;
  }
  private swap(i: number, j: number) {
    [this.heap[i], this.heap[j]] = [this.heap[j], this.heap[i]];
  }
  private bubbleUp(i: number) {
    while (i > 0 && this.heap[this.parent(i)].score > this.heap[i].score) {
      this.swap(i, this.parent(i));
      i = this.parent(i);
    }
  }
  private bubbleDown(i: number) {
    let smallest = i;
    const l = this.left(i),
      r = this.right(i);
    if (l < this.heap.length && this.heap[l].score < this.heap[smallest].score)
      smallest = l;
    if (r < this.heap.length && this.heap[r].score < this.heap[smallest].score)
      smallest = r;
    if (smallest !== i) {
      this.swap(i, smallest);
      this.bubbleDown(smallest);
    }
  }
  findByField<K extends keyof T>(field: K, val: T[K]): number {
    return this.heap.findIndex((item) => item[field] === val);
  }
  insert(item: T): void {
    this.heap.push(item);
    this.bubbleUp(this.heap.length - 1);
  }
  updateScore(index: number, newScore: number | bigint): void {
    const oldScore = this.heap[index].score;
    this.heap[index].score = newScore;
    if (newScore < oldScore) this.bubbleUp(index);
    else this.bubbleDown(index);
  }
  remove(index: number): void {
    const last = this.heap.length - 1;
    this.swap(index, last);
    this.heap.pop();
    if (index < this.heap.length) {
      this.bubbleUp(index);
      this.bubbleDown(index);
    }
  }
  getScore(index: number): number | bigint {
    return this.heap[index].score;
  }
  get(index: number): T {
    return this.heap[index];
  }
  toArray(): T[] {
    return [...this.heap];
  }
  size(): number {
    return this.heap.length;
  }
  toSortedArray(): T[] {
    return [...this.heap].sort((a: any, b: any) => {
      if (a.score !== b.score) return a.score - b.score;
      const aKey = (a as any).value ?? (a as any).member ?? "";
      const bKey = (b as any).value ?? (b as any).member ?? "";
      return aKey < bKey ? -1 : aKey > bKey ? 1 : 0;
    });
  }
}
