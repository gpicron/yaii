import { Source } from 'pull-stream'

export class PullSourceAsyncIterator<T> implements AsyncIterableIterator<T> {
    private source: Source<T>
    private done: boolean = false

    constructor(source: Source<T>) {
        this.source = source
    }

    next(): Promise<IteratorResult<T>> {
        return new Promise<IteratorResult<T>>((resolve, reject) => {
            this.source(null, (endOrError: Error | boolean | null, data: T) => {
                if (endOrError === true) {
                    this.done = true
                    resolve({
                        done: true,
                        value: data
                    })
                }
                if (endOrError != null) reject(endOrError)

                resolve({
                    done: false,
                    value: data
                })
            })
        })
    }

    return(): Promise<IteratorResult<T>> {
        return new Promise<IteratorResult<T>>(resolve => {
            this.source(true, (endOrError: Error | boolean | null, data: T) => {
                resolve({
                    done: true,
                    value: data
                })
            })
        })
    }

    throw(e?: unknown): Promise<IteratorResult<T>> {
        return new Promise<IteratorResult<T>>(resolve => {
            this.source(true, (endOrError: Error | boolean | null, data: T) => {
                resolve({
                    done: true,
                    value: e
                })
            })
        })
    }

    [Symbol.asyncIterator]() {
        return this
    }
}
