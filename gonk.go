package gonk

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"slices"
)

var (
	mutexes         []sync.RWMutex
	growstrategy    GrowStrategy    = FourItems
	reindexstrategy ReindexStrategy = OnGet
)

func init() {
	mutexes = make([]sync.RWMutex, runtime.NumCPU())
}

type Gonk[dataType Evaluable[dataType]] struct {
	blank   dataType
	backing atomic.Pointer[Backing[dataType]]
}

type Evaluable[dataType any] interface {
	Compare(otherEvaluable dataType) int
}

type Backing[dataType Evaluable[dataType]] struct {
	data     []BackingData[dataType]
	maxClean uint32        // Not atomic, this is always only being read (number of sorted items)
	maxTotal atomic.Uint32 // Number of total items
	deleted  atomic.Uint32 // Number of deleted items
	lookups  atomic.Uint32 // Number of serial lookups since created (i.e. not binary searches)
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

// Get one of the shared mutexes
func (g *Gonk[dataType]) mu() *sync.RWMutex {
	return &mutexes[int(uintptr(unsafe.Pointer(g))/unsafe.Sizeof(*g))%len(mutexes)]
}

func (g *Gonk[dataType]) Init(preloadSize int) {
	oldBacking := g.getBacking()
	if oldBacking != nil {
		// That's too late
		return
	}
	g.mu().Lock()
	newBacking := Backing[dataType]{
		data: make([]BackingData[dataType], preloadSize),
	}
	if g.backing.CompareAndSwap(nil, &newBacking) {
		g.init()
	}
	g.mu().Unlock()
}

func (g *Gonk[dataType]) BulkLoad(items []dataType) {
	oldBacking := g.getBacking()
	if oldBacking != nil {
		// That's too late
		panic("gonk: BulkLoad called after init or insertions")
	}
	g.mu().Lock()
	newBacking := Backing[dataType]{
		data:     make([]BackingData[dataType], len(items)),
		maxClean: uint32(len(items)),
	}
	for i := range items {
		newBacking.data[i].item = items[i]
		newBacking.data[i].alive.Store(1)
	}
	newBacking.maxTotal.Store(uint32(len(items)))

	if g.backing.CompareAndSwap(nil, &newBacking) {
		g.init()
		slices.SortFunc(newBacking.data, func(a, b BackingData[dataType]) int { return a.item.Compare(b.item) })
	}
	g.mu().Unlock()
}

func (g *Gonk[dataType]) init() {
}

func SetGrowStrategy(gs GrowStrategy) {
	growstrategy = gs
}

func SetReindexStrategy(rs ReindexStrategy) {
	reindexstrategy = rs
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
	return g.backing.Load()
}

func (g *Gonk[dataType]) search(key dataType) (result *BackingData[dataType]) {
	backing := g.getBacking()
	var lookups int
	var max uint32

	if backing != nil {
		// n, found := slices.BinarySearchFunc(backing.data[:backing.maxClean], BackingData[dataType]{item: key}, func(a, b BackingData[dataType]) int {
		n, found := g.binarysearch(backing.data[:backing.maxClean], key)
		if found {
			return &backing.data[n]
		}
		max = backing.maxTotal.Load()
		for i := backing.maxClean; i < max; i++ {
			// !a<b && !b<a == EQUALS
			lookups++
			if backing.data[i].item.Compare(key) == 0 {
				result = &backing.data[i]
				break
			}
		}
		if lookups > 0 {
			backing.lookups.Add(uint32(lookups))
		}
	}
	return
}

func (g *Gonk[dataType]) binarysearch(b []BackingData[dataType], target dataType) (int, bool) {
	n := len(b)
	// Define cmp(x[-1], target) < 0 and cmp(x[n], target) >= 0 .
	// Invariant: cmp(x[i - 1], target) < 0, cmp(x[j], target) >= 0.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		if b[h].item.Compare(target) < 0 {
			i = h + 1 // preserves cmp(x[i - 1], target) < 0
		} else {
			j = h // preserves cmp(x[j], target) >= 0
		}
	}
	// i == j, cmp(x[i-1], target) < 0, and cmp(x[j], target) (= cmp(x[i], target)) >= 0  =>  answer is i.
	return i, i < n && b[i].item.Compare(target) == 0
}

func (g *Gonk[dataType]) Delete(item dataType) bool {
	g.mu().RLock()
	dataItem := g.search(item)
	var found bool
	if dataItem != nil {
		if dataItem.alive.CompareAndSwap(1, 0) {
			backing := g.getBacking()
			backing.deleted.Add(1)
			found = true
		}
	}
	g.mu().RUnlock()
	g.autooptimize()
	return found
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
	g.mu().RLock()
	dataItem := g.search(target)
	g.mu().RUnlock()
	if dataItem == nil || dataItem.alive.Load() != 1 {
		return g.blank, false
	}
	return dataItem.item, true
}

func (g *Gonk[dataType]) Store(target dataType) {
	g.AtomicMutate(target, func(item *dataType) {
		*item = target
	}, true)
}

func (g *Gonk[dataType]) AtomicMutate(target dataType, mf func(item *dataType), insertIfNotFound bool) {
	g.mu().RLock()
	dataItem := g.search(target)
	if dataItem != nil {
		mf(&dataItem.item)
		if dataItem.alive.CompareAndSwap(0, 1) {
			backing := g.getBacking()
			backing.deleted.Add(^uint32(0))
		}
		g.mu().RUnlock()
		return
	}
	// Not found
	if !insertIfNotFound {
		// Not asked to insert it
		g.mu().RUnlock()
		return
	}

	oldBacking := g.getBacking()
	var oldMax uint32
	if oldBacking != nil {
		oldMax = oldBacking.maxTotal.Load()
	}

	g.mu().RUnlock()

	g.mu().Lock()

	// There was someone else doing changes, maybe they inserted it?
	newBacking := g.getBacking()
	if oldBacking == newBacking && newBacking != nil {
		// Only a few was inserted, so just search those
		newMax := newBacking.maxTotal.Load()

		for i := oldMax; i < newMax; i++ {
			if newBacking.data[i].item.Compare(target) == 0 {
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
		g.mu().Unlock()
		return
	}

	item := g.insert(target)
	mf(item)

	g.mu().Unlock()
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
		g.mu().Lock()
		maybeNewbacking := g.getBacking()
		if maybeNewbacking == backing {
			// should we optimize?
			g.maintainBacking(Same)
		}
		g.mu().Unlock()
	} else if backing.deleted.Load()*4 > backing.maxTotal.Load() {
		// Auto shrink if 25% or more have been deleted
		g.mu().Lock()
		maybeNewbacking := g.getBacking()
		if maybeNewbacking == backing {
			// should we optimize?
			g.maintainBacking(Minimize)
		}
		g.mu().Unlock()
	}
}

func (g *Gonk[dataType]) Optimize(requiredModification sizeModifierFlag) {
	g.mu().Lock()
	g.maintainBacking(requiredModification)
	g.mu().Unlock()
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

	if oldBacking == nil {
		// first time we're getting dirty around there
		g.init()
		newBacking := Backing[dataType]{
			data: make([]BackingData[dataType], 4),
		}
		g.backing.Store(&newBacking)
		return
	}

	oldMax := int(oldBacking.maxTotal.Load())
	oldClean := int(oldBacking.maxClean)
	oldDeleted := int(oldBacking.deleted.Load())
	var newLength int
	switch requestedModification {
	case Grow:
		var growSize int
		switch growstrategy {
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

		slices.SortFunc(insertedData, func(a, b BackingData[dataType]) int { return a.item.Compare(b.item) })

		// Merge old and new
		oldCleanData := oldBacking.data[:int(oldBacking.maxClean)]
		fixData := newData[:insertEnd]
		insertData := newData[insertStart:insertEnd]
		for oc, f, i := 0, 0, 0; oc < len(oldCleanData); {
			if oldCleanData[oc].alive.Load() == 0 {
				oc++
				continue
			}
			if i < len(insertedData) && insertedData[i].item.Compare(oldCleanData[oc].item) < 0 { // Less than
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

		if !g.backing.CompareAndSwap(oldBacking, &newBacking) {
			panic("Backing was changed behind my back")
		}
	} else {
		if !g.backing.CompareAndSwap(oldBacking, nil) {
			panic("Backing was changed behind my back")
		}
	}
}
