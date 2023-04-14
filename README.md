# GONK
... sorry, no - it's no an acronym or anything

*Generic Multipurpose Semi-lockfree Sorted Slice*

As I'm using lots of native Go maps in another project, I was looking for something that used a bit less memory, was concurrent enough for my needs and still was fairly performant. It's a tradeoff between memory usage and performance.

This is what I came up with.

Usage:

```
type MyStoredItem struct {
    id int
}

func (msi *MyStoredItem) LessThan(msi2 *MyStoredItem) bool {
    return msi.id < msi2.id
}

func main() {
    list := gonk.Gonk[MyStoredItem]
    list.Init(10) // Pre-allocate room for 10 items
    list.Set(MyStoredItem{id: 1})
    ...
}
```

##Inner workings:

It consists of various flags and mutexes, and a single pre-allocated slice containing flags for whether the entry is deleted or not and your chosen type of data to contain

These are the playing rules:
- Your provided structure to insert must adhere to the LessThan interface like in the example above
- Ranging over items are lock free, and returns best-effort data (might be the same as when you started the Range, or might change underway, no guarantees)
- You're allowed to do changes to existing data by multiple routines at the same time (allows doing atomic updates if you can handle that, but YOU have to handle this yourself)
- Multiple routines can insert new data at the same time
- When the slice is resizing/optimizing it locks exclusively and automatically

The slice is split in two parts:
- A "stable" part with existing data. This part is sorted using the LessThan method, and allows for binary search lookups
- A "new" part with items in random order. This part is bruteforce searched for matching keys.

This is not for everyone, but it was exactly what I needed myself ;-)
