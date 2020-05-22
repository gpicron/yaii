
export interface LRUCache<T> {
    put: (key: string, value: T) => void;
    get: (key: string, factory?: () => T | Promise<T>) => T | undefined | Promise<T | undefined>;
}

export interface ValueWithMemoryEstimation {
    readonly sizeInMemory: number
}

interface IEntry<T> {
    newer?: IEntry<T>;
    older?: IEntry<T>;
    key: string;
    value: T;
}


export class MemoryLimitedLRUCache<T extends ValueWithMemoryEstimation> implements LRUCache<T>{
    // Current rangeSize of the cache. (Read-only).
    private size = 0
    // Maximum number of items this cache can hold.
    limit: number;
    private _keymap = new Map<string, IEntry<T>>();

    private tail: IEntry<T> | undefined
    private head: IEntry<T> | undefined

    constructor (limit: number) {
        this.limit = limit
    }

    async get(key: string, factory?: () => T | Promise<T>) {
        // First, find our cache entry
        const entry = this._keymap.get(key);
        if (entry === undefined) {
            if (factory){
                const v = await factory()
                this.put(key, v)

                return v
            }
            // Not cached. Sorry.
            return undefined;
        }
        // As <key> was found in the cache, register it as being requested recently
        if (entry === this.tail) {
            // Already the most recently used entry, so no need to update the list
            return entry.value;
        }
        // HEAD--------------TAIL
        //   <.older   .newer>
        //  <--- add direction --
        //   A  B  C  <D>  E
        if (entry.newer) {
            if (entry === this.head)
                this.head = entry.newer;
            entry.newer.older = entry.older; // C <-- E.
        }
        if (entry.older)
            entry.older.newer = entry.newer; // C. --> E
        entry.newer = undefined; // D --x
        entry.older = this.tail; // D. --> E
        if (this.tail)
            this.tail.newer = entry; // E. <-- D
        this.tail = entry;
        return entry.value;
    }

    put(key: string, value: T): void {
        const entry: IEntry<T> = { key: key, value: value };
        // Note: No protection against replacing, and thus orphan entries. By design.
        this._keymap.set(key, entry);
        if (this.tail) {
            // link previous tail to the new tail (entry)
            this.tail.newer = entry;
            entry.older = this.tail;
        } else {
            // we're first in -- yay
            this.head = entry;
        }
        // add new entry to the end of the linked list -- it's now the freshest entry.
        this.tail = entry;
        this.size +=  value.sizeInMemory

        while (this.size >= this.limit) {
            this.shift();
        }
    }

    private shift() {
        const entry = this.head;
        if (entry) {
            if (entry.newer) {
                this.head = entry.newer;
                this.head.older = undefined;
            } else {
                this.head = undefined;
            }
            // Remove last strong reference to <entry> and remove links from the purged
            // entry being returned:
            entry.newer = entry.older = undefined;
            // delete is slow, but we need to do this to avoid uncontrollable growth:
            this._keymap.delete(entry.key);
        }
        return entry;
    }
}

