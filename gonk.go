package gonk

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/peterrk/slices"
)

type Gonk[dataType Evaluable[dataType]] struct {
	backing         unsafe.Pointer
	sorter          slices.Order[BackingData[dataType]]
	mu              sync.RWMutex
	growing         atomic.Uint32
	growstrategy    GrowStrategy
	reindexstrategy ReindexStrategy
}

type Evaluable[dataType any] interface {
	LessThan(otherEvaluable dataType) bool
}

type Backing[dataType Evaluable[dataType]] struct {
	data     []BackingData[dataType]
	maxClean uint32        // Not atomic, this is always only being read (number of sorted items)
	maxTotal atomic.Uint32 // Number of total items
	deleted  atomic.Uint32 // Number of deleted items
	lookups  atomic.Uint32 // Number of lookups since created
}

type BackingData[dataType Evaluable[dataType]] struct {
	alive atomic.Uint32
	item  dataType
}

type GrowStrategy uint8

const (
	Double GrowStrategy = iota
	Half
	HalfMax2048
	OneAtATime
	FourItems
)

type ReindexStrategy uint8

const (
	Adaptive ReindexStrategy = iota
	OnGet
)

func (g *Gonk[dataType]) Init(preloadSize int) {
	oldBacking := g.getBacking()
	if oldBacking != nil {
		// That's too late
		return
	}
	g.mu.Lock()
	newBacking := Backing[dataType]{
		data: make([]BackingData[dataType], preloadSize),
	}
	if atomic.CompareAndSwapPointer(&g.backing, unsafe.Pointer(nil), unsafe.Pointer(&newBacking)) {
		g.init()
	}
	g.mu.Unlock()
}

func (g *Gonk[dataType]) init() {
	g.sorter = slices.Order[BackingData[dataType]]{
		RefLess: func(a, b *BackingData[dataType]) bool {
			return a.item.LessThan(b.item)
		},
	}
}

func (g *Gonk[dataType]) GrowStrategy(gs GrowStrategy) {
	g.mu.Lock()
	g.growstrategy = gs
	g.mu.Unlock()
}

func (g *Gonk[dataType]) ReindexStrategy(rs ReindexStrategy) {
	g.mu.Lock()
	g.reindexstrategy = rs
	g.mu.Unlock()
}

func (g *Gonk[dataType]) Range(af func(item dataType) bool) {
	backing := g.getBacking()
	if backing == nil {
		return
	}
	last := int(backing.maxTotal.Load())
	if last == 0 {
		return
	}

	data := backing.data[:last]
	for i := range data {
		if data[i].alive.Load() == 1 {
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
		backing.lookups.Add(1)
		n, found := g.sorter.BinarySearch(backing.data[:backing.maxClean], BackingData[dataType]{item: key})
		if found {
			return &backing.data[n]
		}
		max := backing.maxTotal.Load()
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
		if dataItem.alive.CompareAndSwap(1, 0) {
			backing := g.getBacking()
			backing.deleted.Add(1)
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

	return int(backing.maxTotal.Load() - backing.deleted.Load())
}

func (g *Gonk[dataType]) PreciseLen() int {
	backing := g.getBacking()
	if backing == nil {
		return 0
	}
	var length int
	max := int(backing.maxTotal.Load())
	for i := 0; i < max && i < len(backing.data); /* BCE */ i++ {
		if backing.data[i].alive.Load() == 1 {
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
		if dataItem.alive.CompareAndSwap(0, 1) {
			backing := g.getBacking()
			backing.deleted.Add(^uint32(0))
		}
		g.mu.RUnlock()
		return
	}
	// Not found
	if !insertIfNotFound {
		// Not asked to insert it
		g.mu.RUnlock()
		return
	}

	oldBacking := g.getBacking()
	var oldMax uint32
	if oldBacking != nil {
		oldMax = oldBacking.maxTotal.Load()
	}

	g.mu.RUnlock()

	g.mu.Lock()

	// There was someone else doing changes, maybe they inserted it?
	newBacking := g.getBacking()
	if oldBacking == newBacking && newBacking != nil {
		// Only a few was inserted, so just search those
		newMax := newBacking.maxTotal.Load()

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
		if dataItem.alive.CompareAndSwap(0, 1) {
			backing := g.getBacking()
			backing.deleted.Add(^uint32(0))
		}
		g.mu.Unlock()
		return
	}

	item := g.insert(target)
	mf(item)

	g.mu.Unlock()
	g.autooptimize()
}

func (g *Gonk[dataType]) insert(data dataType) *dataType {
	backing := g.getBacking()

	for backing == nil || int(backing.maxTotal.Load()) == cap(backing.data) {
		g.maintainBacking(Grow)
		backing = g.getBacking()
	}

	newMax := backing.maxTotal.Add(1)
	backing.data[int(newMax-1)].item = data
	backing.data[int(newMax-1)].alive.Store(1)

	return &backing.data[int(newMax-1)].item
}

func (g *Gonk[dataType]) autooptimize() {
	backing := g.getBacking()
	if backing == nil {
		return
	}

	dirtyCount := backing.maxTotal.Load() - atomic.LoadUint32(&backing.maxClean)
	lookups := backing.lookups.Load()

	if dirtyCount > 64 && lookups*dirtyCount > 1<<24 { // More than 1<<20 (16m) wasted loops
		// Auto reindex
		g.mu.Lock()
		maybeNewbacking := g.getBacking()
		if maybeNewbacking == backing {
			// should we optimize?
			g.maintainBacking(Same)
		}
		g.mu.Unlock()
	} else if backing.deleted.Load()*4 > backing.maxTotal.Load() {
		// Auto shrink if 25% or more have been deleted
		g.mu.Lock()
		maybeNewbacking := g.getBacking()
		if maybeNewbacking == backing {
			// should we optimize?
			g.maintainBacking(Minimize)
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

	if requestedModification == Same && (oldBacking == nil || oldBacking.maxClean == oldBacking.maxTotal.Load()) {
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

	oldMax := int(oldBacking.maxTotal.Load())
	oldClean := int(oldBacking.maxClean)
	oldDeleted := int(oldBacking.deleted.Load())
	var newLength int
	switch requestedModification {
	case Grow:
		var growSize int
		switch g.growstrategy {
		case OneAtATime:
			growSize = 1
		case FourItems:
			growSize = 4
		case Double:
			growSize = oldMax
		case Half:
			growSize = oldMax / 2
		case HalfMax2048:
			growSize = oldMax / 2
			if growSize > 2048 {
				growSize = 2048
			}
		}
		if growSize < 2 {
			growSize = 2
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
				if oldDirtyData[i].alive.Load() == 0 {
					continue
				}
				insertStart--
				newData[insertStart].item = oldDirtyData[i].item
				newData[insertStart].alive.Store(1)
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
			if oldCleanData[oc].alive.Load() == 0 {
				oc++
				continue
			}
			if i < len(insertedData) && insertedData[i].item.LessThan(oldCleanData[oc].item) {
				fixData[f].item = insertData[i].item
				fixData[f].alive.Store(1)
				i++
			} else {
				fixData[f].item = oldCleanData[oc].item
				fixData[f].alive.Store(1)
				oc++
			}
			f++
		}

		newBacking := Backing[dataType]{
			data:     newData,
			maxClean: uint32(insertEnd),
		}
		newBacking.maxTotal.Store(uint32(insertEnd))

		if !atomic.CompareAndSwapPointer(&g.backing, unsafe.Pointer(oldBacking), unsafe.Pointer(&newBacking)) {
			panic("Backing was changed behind my back")
		}
	} else {
		if !atomic.CompareAndSwapPointer(&g.backing, unsafe.Pointer(oldBacking), unsafe.Pointer(nil)) {
			panic("Backing was changed behind my back")
		}
	}
	g.growing.Store(0)
}
