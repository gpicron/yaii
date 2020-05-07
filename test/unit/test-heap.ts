import anyTest, {ExecutionContext, Implementation, TestInterface} from 'ava'
import Heap from "../../src/lib/internal/datastructs/binary-heap"

interface TestContext {
    heap: Heap<any>
}

const test = anyTest as TestInterface<TestContext>

function customCompare(a: any, b: any) {
    if (a.val < b.val) {
        return -1;
    } else if (a.val === b.val) {
        return 0;
    } else {
        return 1;
    }
}


test.beforeEach(t => {
    t.context.heap = new Heap<any>(((a, b) => (a < b) ? -1 : (a === b) ? 0 : 1));
})

var createHeap1 = function (t: ExecutionContext<TestContext>) {
    t.context.heap.add(0);
    t.context.heap.add(1);
    t.context.heap.add(2);
    t.context.heap.add(3);
};

var createHeap2 = function (t: ExecutionContext<TestContext>) {
    t.context.heap.add(1);
    t.context.heap.add(3);
    t.context.heap.add(0);
    t.context.heap.add(2);
};

var createHeap3 = function (t: ExecutionContext<TestContext>) {
    t.context.heap.add('a');
    t.context.heap.add('b');
    t.context.heap.add('c');
    t.context.heap.add('d');
};

var createHeap4 = function (t: ExecutionContext<TestContext>) {
    t.context.heap.add('b');
    t.context.heap.add('d');
    t.context.heap.add('a');
    t.context.heap.add('c');
};

var createHeap5 = function (t: ExecutionContext<TestContext>) {
    t.context.heap.add({val: 'b'});
    t.context.heap.add({val: 'd'});
    t.context.heap.add({val: 'a'});
    t.context.heap.add({val: 'c'});
};


test('Gives the right size 1',
    t => {
        createHeap1(t);
        t.assert(t.context.heap.size()== 4);
        t.context.heap.removeRoot();
        t.assert(t.context.heap.size()== 3);
    });

test('Gives the right size 2',
    t => {
        createHeap1(t);
        t.context.heap.removeRoot();
        t.context.heap.removeRoot();
        t.context.heap.removeRoot();
        t.context.heap.removeRoot();
        t.assert(t.context.heap.size()== 0);
    });

test('Gives the right size with strings',
    t => {
        createHeap1(t);
        t.context.heap.removeRoot();
        t.context.heap.removeRoot();
        t.context.heap.removeRoot();
        t.context.heap.removeRoot();
        t.assert(t.context.heap.size()== 0);
    });

test('Peeks the lowest element',
    t => {
        createHeap1(t);
        t.assert(t.context.heap.peek()== 0);
        t.context.heap.clear();
        t.assert(t.context.heap.peek()== undefined);
    });

test('Peeks the lowest element 2',
    t => {
        createHeap1(t);
        t.assert(t.context.heap.peek()== 0);
    });

test('Peeks the lowest element with strings',
    t => {
        createHeap3(t);
        t.assert(t.context.heap.peek()== 'a');
    });

test('Peeks the lowest element with strings 2',
    t => {
        createHeap3(t);
        t.assert(t.context.heap.peek()== 'a');
    });

test('Peeks the lowest element with custom objects',
    t => {
        t.context.heap = new Heap(customCompare);
        createHeap5(t);
        t.assert(t.context.heap.peek().val == 'a');
    });

test('Removes root',
    t => {
        createHeap1(t);
        t.assert(t.context.heap.removeRoot()== 0);
        t.assert(t.context.heap.removeRoot()== 1);
        t.assert(t.context.heap.removeRoot()== 2);
        t.assert(t.context.heap.removeRoot()== 3);
    });

test('Removes root 2',
    t => {
        createHeap1(t);
        t.context.heap.add(1);
        t.assert(t.context.heap.removeRoot()== 0);
        t.assert(t.context.heap.removeRoot()== 1);
        t.assert(t.context.heap.removeRoot()== 1);
        t.assert(t.context.heap.removeRoot()== 2);
        t.assert(t.context.heap.removeRoot()== 3);
    });

test('Removes root with custom objects',
    t => {
        t.context.heap = new Heap(customCompare);
        createHeap5(t);
        t.assert(t.context.heap.removeRoot().val== 'a');
        t.assert(t.context.heap.removeRoot().val== 'b');
        t.assert(t.context.heap.removeRoot().val== 'c');
        t.assert(t.context.heap.removeRoot().val== 'd');
    });

test('Adds and peeks',
    t => {
        t.context.heap.add(3);
        t.assert(t.context.heap.peek()== 3);
        t.context.heap.add(2);
        t.assert(t.context.heap.peek()== 2);
        t.context.heap.add(1);
        t.assert(t.context.heap.peek()== 1);
        t.context.heap.add(0);
        t.assert(t.context.heap.peek()== 0);
    });

test('Adds and peeks 2',
    t => {
        t.context.heap.add(1);
        t.assert(t.context.heap.peek()== 1);
        t.context.heap.add(3);
        t.assert(t.context.heap.peek()== 1);
        t.context.heap.add(0);
        t.assert(t.context.heap.peek()== 0);
        t.context.heap.add(2);
        t.assert(t.context.heap.peek()== 0);
    });

test('An empty heap is empty',
    t => {
        t.assert(t.context.heap.isEmpty()== true);
        createHeap1(t);
        for (var i = 0; i < t.context.heap.size(); i++) {
            t.assert(t.context.heap.isEmpty()== false);
            t.context.heap.removeRoot();
        }
    });

test('Clear removes all elements',
    t => {
        t.context.heap.clear();
        createHeap1(t);
        t.context.heap.clear();
        t.assert(t.context.heap.isEmpty()== true);
        t.assert(t.context.heap.peek()== undefined);
    });


test('Contains inserted elements',
    t => {
        createHeap1(t);
        for (var i = 0; i < 4; i++) {
            t.assert(t.context.heap.contains(i)== true);
        }
        t.assert(t.context.heap.contains(i)== false);
    });
