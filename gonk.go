package gonk

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/peterrk/slices"
)

type Gonk[dataType Evaluable[dataType]] struct {
	backing unsafe.Pointer
	sorter  slices.Order[BackingData[dataType]]
	mu      sync.RWMutex
	growing atomic.Uint32
}

type Evaluable[dataType any] interface {
	LessThan(otherEvaluable dataType) bool
}

type Backing[dataType Evaluable[dataType]] struct {
	data     []BackingData[dataType]
	maxClean uint32 // Not atomic, this is always only being read (number of sorted items)
	maxTotal uint32 // Number of total items
	deleted  uint32 // Number of deleted items
	lookups  uint32 // Number of lookups since created
}

type BackingData[dataType Evaluable[dataType]] struct {
	alive uint32
	item  dataType
}

func (g *Gonk[dataType]) init() {
	g.sorter = slices.Order[BackingData[dataType]]{
		RefLess: func(a, b *BackingData[dataType]) bool {
			return a.item.LessThan(b.item)
		},
	}
}

func (g *Gonk[dataType]) Range(af func(item dataType) bool) {
	backing := g.getBacking()
	if backing == nil {
		return
	}
	last := int(atomic.LoadUint32(&backing.maxTotal))
	if last == 0 {
		return
	}

	data := backing.data[:last]
	for i := range data {
		if atomic.LoadUint32(&data[i].alive) == 1 {
			if !af(data[i].item) {
				break
			}
		}
	}
}

func (g *Gonk[dataType]) getBacking() *Backing[dataType] {
	return (*Backing[dataType])(atomic.LoadPointer(&g.backing))
}

func (g *Gonk[dataType]) search(key dataType) *BackingData[dataType] {
	backing := g.getBacking()
	if backing != nil {
		atomic.AddUint32(&backing.lookups, 1)
		n, found := g.sorter.BinarySearch(backing.data[:backing.maxClean], BackingData[dataType]{item: key})
		if found {
			return &backing.data[n]
		}
		max := atomic.LoadUint32(&backing.maxTotal)
		for i := backing.maxClean; i < max; i++ {
			// !a<b && !b<a == EQUALS
			if !backing.data[i].item.LessThan(key) && !key.LessThan(backing.data[i].item) {
				return &backing.data[i]
			}
		}
	}
	return nil
}

func (g *Gonk[dataType]) Delete(item dataType) bool {
	g.mu.RLock()
	dataItem := g.search(item)
	if dataItem != nil {
		if atomic.CompareAndSwapUint32(&dataItem.alive, 1, 0) {
			backing := g.getBacking()
			atomic.AddUint32(&backing.deleted, 1)
			g.mu.RUnlock()
			g.autooptimize()
			return true
		}
	}
	g.mu.RUnlock()
	g.autooptimize()
	return false
}

func (g *Gonk[dataType]) Len() int {
	backing := g.getBacking()
	if backing == nil {
		return 0
	}

	return int(atomic.LoadUint32(&backing.maxTotal) - atomic.LoadUint32(&backing.deleted))
}

func (g *Gonk[dataType]) preciseLen() int {
	backing := g.getBacking()
	if backing == nil {
		return 0
	}
	var length int
	max := int(atomic.LoadUint32(&backing.maxTotal))
	for i := 0; i < max && i < len(backing.data); /* BCE */ i++ {
		if atomic.LoadUint32(&backing.data[i].alive) == 1 {
			length++
		}
	}
	return length
}

func (g *Gonk[dataType]) Load(target dataType) (dataType, bool) {
	g.mu.RLock()
	dataItem := g.search(target)
	g.mu.RUnlock()
	if dataItem == nil {
		return *new(dataType), false
	}
	return dataItem.item, true
}

func (g *Gonk[dataType]) Store(target dataType) {
	g.AtomicMutate(target, func(item *dataType) {
		*item = target
	}, true)
}

func (g *Gonk[dataType]) AtomicMutate(target dataType, mf func(item *dataType), insertIfNotFound bool) {
	g.mu.RLock()
	dataItem := g.search(target)
	if dataItem != nil {
		mf(&dataItem.item)
		if atomic.CompareAndSwapUint32(&dataItem.alive, 0, 1) {
			backing := g.getBacking()
			atomic.AddUint32(&backing.deleted, ^uint32(0))
		}
		g.mu.RUnlock()
		return
	}
	// Not found
	if !insertIfNotFound {
		// Not asked to insert it
		return
	}

	oldBacking := g.getBacking()
	var oldMax uint32
	if oldBacking != nil {
		oldMax = atomic.LoadUint32(&oldBacking.maxTotal)
	}

	g.mu.RUnlock()

	g.mu.Lock()

	// There was someone else doing changes, maybe they inserted it?
	newBacking := g.getBacking()
	if oldBacking == newBacking && newBacking != nil {
		// Only a few was inserted, so just search those
		newMax := atomic.LoadUint32(&newBacking.maxTotal)

		for i := oldMax; i < newMax; i++ {
			if !newBacking.data[i].item.LessThan(target) && !target.LessThan(newBacking.data[i].item) {
				dataItem = &newBacking.data[i]
			}
		}
	} else {
		// the backing was switched, so search again
		dataItem = g.search(target)
	}

	if dataItem != nil {
		mf(&dataItem.item)
		if atomic.CompareAndSwapUint32(&dataItem.alive, 0, 1) {
			backing := g.getBacking()
			atomic.AddUint32(&backing.deleted, ^uint32(0))
		}
		g.mu.Unlock()
		return
	}

	var newitem dataType
	item := g.insert(newitem)
	mf(item)

	g.mu.Unlock()
	g.autooptimize()
}

func (g *Gonk[dataType]) insert(data dataType) *dataType {
	newDataItem := BackingData[dataType]{
		alive: 1,
		item:  data,
	}

	backing := g.getBacking()

	for backing == nil || int(atomic.LoadUint32(&backing.maxTotal)) == len(backing.data) {
		g.maintainBacking(Grow)
		backing = g.getBacking()
	}

	newMax := atomic.AddUint32(&backing.maxTotal, 1)
	backing.data[int(newMax-1)] = newDataItem

	return &backing.data[int(newMax-1)].item
}

func (g *Gonk[dataType]) autooptimize() {
	backing := g.getBacking()
	if backing == nil {
		return
	}

	dirtyCount := atomic.LoadUint32(&backing.maxTotal) - atomic.LoadUint32(&backing.maxClean)
	lookups := atomic.LoadUint32(&backing.lookups)

	if dirtyCount > 64 && lookups*dirtyCount > 1<<24 { // More than 1<<20 (16m) wasted loops
		g.mu.Lock()
		maybeNewbacking := g.getBacking()
		if maybeNewbacking == backing {
			// should we optimize?
			g.maintainBacking(Same)
		}
		g.mu.Unlock()
	}
}

func (g *Gonk[dataType]) Optimize(requiredModification sizeModifierFlag) {
	g.mu.Lock()
	g.maintainBacking(requiredModification)
	g.mu.Unlock()
}

type sizeModifierFlag uint8

const (
	Grow sizeModifierFlag = iota
	Same
	Minimize
)

func (g *Gonk[dataType]) maintainBacking(requestedModification sizeModifierFlag) {
	oldBacking := g.getBacking()

	if requestedModification == Same && (oldBacking == nil || oldBacking.maxClean == oldBacking.maxTotal) {
		// Already optimal
		return
	}

	if !g.growing.CompareAndSwap(0, 1) {
		panic("growing twice")
	}

	if oldBacking == nil {
		// first time we're getting dirty around there
		g.init()
		newBacking := Backing[dataType]{
			data: make([]BackingData[dataType], 4),
		}
		atomic.StorePointer(&g.backing, unsafe.Pointer(&newBacking))
		g.growing.Store(0)
		return
	}

	oldMax := int(atomic.LoadUint32(&oldBacking.maxTotal))
	oldClean := int(oldBacking.maxClean)
	oldDeleted := int(atomic.LoadUint32(&oldBacking.deleted))
	var newLength int
	switch requestedModification {
	case Grow:
		growSize := oldMax / 2
		if growSize > 2048 {
			growSize = 2048
		}
		newLength = oldMax + growSize
	case Same:
		newLength = len(oldBacking.data)
	case Minimize:
		newLength = oldMax - oldDeleted
	}

	if newLength > 0 {
		newData := make([]BackingData[dataType], newLength)

		// Place new non-deleted items at the end of the soon-to-be sorted part of the new slice
		insertEnd := oldMax - oldDeleted
		insertStart := insertEnd
		oldDirtyData := oldBacking.data[int(oldBacking.maxClean):oldMax]
		if oldDeleted == 0 {
			// Nothing was deleted, so just bulk copy it
			insertStart = insertEnd - (oldMax - oldClean)
			copy(newData[insertStart:insertEnd], oldDirtyData)
		} else {
			// Pick non-deleted items one by one
			for i := range oldDirtyData {
				if oldDirtyData[i].alive == 0 {
					continue
				}
				insertStart--
				newData[insertStart] = oldDirtyData[i]
			}
		}

		// Sort the new items
		insertedData := newData[insertStart:insertEnd]

		g.sorter.Sort(insertedData)

		// Merge old and new
		oldCleanData := oldBacking.data[:int(oldBacking.maxClean)]
		fixData := newData[:insertEnd]
		insertData := newData[insertStart:insertEnd]
		for oc, f, i := 0, 0, 0; oc < len(oldCleanData); {
			if oldCleanData[oc].alive == 0 {
				oc++
				continue
			}
			if i < len(insertedData) && insertedData[i].item.LessThan(oldCleanData[oc].item) {
				fixData[f] = insertData[i]
				i++
			} else {
				fixData[f] = oldCleanData[oc]
				oc++
			}
			f++
		}

		newBacking := Backing[dataType]{
			data:     newData,
			maxClean: uint32(insertEnd),
			maxTotal: uint32(insertEnd),
		}

		if !atomic.CompareAndSwapPointer(&g.backing, unsafe.Pointer(oldBacking), unsafe.Pointer(&newBacking)) {
			panic("Backing was changed behind my back")
		}
	} else {
		if !atomic.CompareAndSwapPointer(&g.backing, unsafe.Pointer(oldBacking), unsafe.Pointer(uintptr(0))) {
			panic("Backing was changed behind my back")
		}
	}
	g.growing.Store(0)
}
