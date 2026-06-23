/**
 * Streaming parser that splits text containing <think>...</think> tags into
 * two separate streams: `reasoning` (text inside <think>) and `content`
 * (text outside <think>).
 *
 * The Sakana Namazu model does not emit native reasoning events, so we
 * instruct it (via system prompt) to wrap its reasoning in <think> tags.
 * This parser then converts that into the OpenAI-compatible `reasoning_content`
 * field that clients like OpenAI SDK, Cline, Roo Code, etc. expect.
 *
 * State machine:
 *   - OUTSIDE: text goes to `content` until we see `<think>`
 *   - INSIDE:  text goes to `reasoning` until we see `</think>`
 *
 * Edge cases handled:
 *   - Partial tag at chunk boundary (e.g. `<thi` then `nk>`) — buffered
 *   - Stream ends while INSIDE — flush remaining buffer as reasoning
 *   - Multiple <think>...</think> blocks — each contributes to reasoning
 *   - `<think>` without closing — flush everything after as reasoning
 */
export class ThinkTagParser {
  private buffer = ''
  private insideThink = false
  private pendingPartial = ''

  feed(chunk: string): { content: string; reasoning: string } {
    this.buffer += chunk
    let content = ''
    let reasoning = ''

    while (this.buffer.length > 0) {
      if (!this.insideThink) {
        // Look for <think> opening tag
        const openIdx = this.findTag(this.buffer, '<think>')
        if (openIdx === -1) {
          // Check if buffer ends with a partial <think prefix
          const partialLen = this.getPartialTagLength(this.buffer, '<think>')
          if (partialLen > 0) {
            // Emit everything before the partial, keep the partial buffered
            const safe = this.buffer.slice(0, this.buffer.length - partialLen)
            content += safe
            this.pendingPartial = this.buffer.slice(this.buffer.length - partialLen)
            this.buffer = ''
          } else {
            // No partial — emit everything
            content += this.buffer
            this.buffer = ''
            this.pendingPartial = ''
          }
          break
        }
        // Emit content before <think>
        content += this.buffer.slice(0, openIdx)
        this.buffer = this.buffer.slice(openIdx + '<think>'.length)
        this.insideThink = true
        this.pendingPartial = ''
      } else {
        // Look for </think> closing tag
        const closeIdx = this.findTag(this.buffer, '</think>')
        if (closeIdx === -1) {
          const partialLen = this.getPartialTagLength(this.buffer, '</think>')
          if (partialLen > 0) {
            const safe = this.buffer.slice(0, this.buffer.length - partialLen)
            reasoning += safe
            this.pendingPartial = this.buffer.slice(this.buffer.length - partialLen)
            this.buffer = ''
          } else {
            reasoning += this.buffer
            this.buffer = ''
            this.pendingPartial = ''
          }
          break
        }
        // Emit reasoning before </think>
        reasoning += this.buffer.slice(0, closeIdx)
        this.buffer = this.buffer.slice(closeIdx + '</think>'.length)
        this.insideThink = false
        this.pendingPartial = ''
      }
    }

    return { content, reasoning }
  }

  flush(): { content: string; reasoning: string } {
    const remaining = this.buffer + this.pendingPartial
    this.buffer = ''
    this.pendingPartial = ''

    if (!remaining) return { content: '', reasoning: '' }

    if (this.insideThink) {
      // Stream ended inside <think> — flush as reasoning
      this.insideThink = false
      return { content: '', reasoning: remaining }
    }
    // Stream ended outside — flush as content
    return { content: remaining, reasoning: '' }
  }

  isInsideThink(): boolean {
    return this.insideThink
  }

  /**
   * Finds the next occurrence of `tag` in `text`, case-insensitive.
   * Returns -1 if not found.
   */
  private findTag(text: string, tag: string): number {
    const lower = text.toLowerCase()
    const tagLower = tag.toLowerCase()
    return lower.indexOf(tagLower)
  }

  /**
   * Returns the length of a partial `tag` prefix at the end of `text`.
   * E.g. if text ends with `<thi` and tag is `<think>`, returns 4.
   * Returns 0 if no partial match.
   */
  private getPartialTagLength(text: string, tag: string): number {
    const lower = text.toLowerCase()
    const tagLower = tag.toLowerCase()
    // Check progressively smaller prefixes of the tag
    for (let i = Math.min(tagLower.length - 1, lower.length); i >= 1; i--) {
      const prefix = tagLower.slice(0, i)
      if (lower.endsWith(prefix)) {
        return i
      }
    }
    return 0
  }
}
