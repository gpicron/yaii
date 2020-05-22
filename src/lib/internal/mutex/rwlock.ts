import {Lock} from "./lock"

export class RWLock {
    private r = new Lock()
    private g = new Lock()
    private counter = 0

    async acquireRead(): Promise<void> {
        await this.r.acquire()
        this.counter++

        if (this.counter == 1) await this.g.acquire()

        this.r.release()
    }

    async releaseRead(): Promise<void> {
        await this.r.acquire()
        this.counter--

        if (this.counter == 0) this.g.release()

        this.r.release()
    }

    async acquireWrite(): Promise<void> {
        await this.g.acquire()
    }

    async releaseWrite(): Promise<void> {
        this.g.release()
    }


    async dispatchRead<T>(fn: () => T): Promise<T> {
        await this.acquireRead();

        try {
            return fn()
        } finally {
            this.releaseRead()
        }
    }

    async dispatchWrite<T>(fn: () => T): Promise<T> {
        await this.acquireWrite();

        try {
            return fn()
        } finally {
            this.releaseWrite()
        }
    }

}
