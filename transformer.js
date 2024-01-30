
/**
 * transformer that transforms each input to N outputs as yielded by a
 * fn (I) => AsyncIterable<O>
 * @template I
 * @template O
 * @implements {Transformer<I, O>}
 */
export class SpreadTransformer {
  /**
   * @param {(input: I) => AsyncIterable<O>} spread - convert an input to zero or more outputs
   */
  constructor(spread) {
    this.spread = spread
  }
  async transform(input, controller) {
    for await (const output of this.spread(input)) {
      controller.enqueue(output)
    }
  }
}
