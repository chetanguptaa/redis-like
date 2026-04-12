import type { TZSet } from "../types";

export class MinHeap {
  private heap: TZSet[] = [];
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
  findByValue(value: string): number {
    return this.heap.findIndex((item) => item.value === value);
  }
  insert(item: TZSet): void {
    this.heap.push(item);
    this.bubbleUp(this.heap.length - 1);
  }
  updateScore(index: number, newScore: number): void {
    const oldScore = this.heap[index].score;
    this.heap[index].score = newScore;
    if (newScore < oldScore) this.bubbleUp(index);
    else this.bubbleDown(index);
  }
  toArray(): TZSet[] {
    return [...this.heap];
  }
  size(): number {
    return this.heap.length;
  }
  toSortedArray(): TZSet[] {
    return [...this.heap].sort((a, b) => {
      if (a.score !== b.score) return a.score - b.score;
      return a.value < b.value ? -1 : a.value > b.value ? 1 : 0;
    });
  }
  getScore(index: number): number {
    return this.heap[index].score;
  }
}
