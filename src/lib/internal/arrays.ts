/**
 * Swaps the elements at the specified positions in the specified array.
 * @param {Array} array The array in which to swap elements.
 * @param {number} i the index of one element to be swapped.
 * @param {number} j the index of the other element to be swapped.
 * @return {boolean} true if the array is defined and the indexes are valid.
 */
import {IEqualsFunction} from "./utils"

export function swap<T>(array: T[], i: number, j: number): boolean {
    if (i < 0 || i >= array.length || j < 0 || j >= array.length) {
        return false;
    }
    const temp = array[i];
    array[i] = array[j];
    array[j] = temp;
    return true;
}

/**
 * Returns true if the specified array contains the specified element.
 * @param {*} array the array in which to search the element.
 * @param {Object} item the element to search.
 * @param {function(Object,Object):boolean=} equalsFunction optional function to
 * check equality between 2 elements.
 * @return {boolean} true if the specified array contains the specified element.
 */
export function contains<T>(array: T[], item: T, equalsFunction?: IEqualsFunction<T>): boolean {
    return indexOf(array, item, equalsFunction) >= 0;
}


/**
 * Returns the position of the first occurrence of the specified item
 * within the specified array.4
 * @param {*} array the array in which to search the element.
 * @param {Object} item the element to search.
 * @param {function(Object,Object):boolean=} equalsFunction optional function used to
 * check equality between 2 elements.
 * @return {number} the position of the first occurrence of the specified element
 * within the specified array, or -1 if not found.
 */
export function indexOf<T>(array: T[], item: T, equalsFunction?: IEqualsFunction<T>): number {
    const equals = equalsFunction || ((a: T, b: T) => a === b);
    const length = array.length;
    for (let i = 0; i < length; i++) {
        if (equals(array[i], item)) {
            return i;
        }
    }
    return -1;
}

/**
 * Returns the position of the last occurrence of the specified element
 * within the specified array.
 * @param {*} array the array in which to search the element.
 * @param {Object} item the element to search.
 * @param {function(Object,Object):boolean=} equalsFunction optional function used to
 * check equality between 2 elements.
 * @return {number} the position of the last occurrence of the specified element
 * within the specified array or -1 if not found.
 */
export function lastIndexOf<T>(array: T[], item: T, equalsFunction?: IEqualsFunction<T>): number {
    const equals = equalsFunction || ((a: T, b: T) => a === b);
    const length = array.length;
    for (let i = length - 1; i >= 0; i--) {
        if (equals(array[i], item)) {
            return i;
        }
    }
    return -1;
}


export function removeAll<T>(array: T[], toRemove: T[], equalsFunction?: IEqualsFunction<T>): T[] {
    const equals = equalsFunction || ((a: T, b: T) => a === b);
    const length = array.length;
    const result = []
    for (let i = 0; i < length; i++) {
        const item = array[i]
        if (!contains(toRemove, item, equals)) {
            result.push(item)
        }
    }
    return result;
}
