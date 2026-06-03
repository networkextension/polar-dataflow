package core

import "sync"

// Registry holds all plugins registered with the framework. Verticals
// register their plugins at startup; the pipeline reads from here.
type Registry struct {
	mu          sync.RWMutex
	classifiers []Classifier
	processors  []Processor
	evaluators  []Evaluator
	dispatchers []Dispatcher
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) RegisterClassifier(c Classifier) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.classifiers = append(r.classifiers, c)
}

func (r *Registry) RegisterProcessor(p Processor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.processors = append(r.processors, p)
}

func (r *Registry) RegisterEvaluator(e Evaluator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evaluators = append(r.evaluators, e)
}

func (r *Registry) RegisterDispatcher(d Dispatcher) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dispatchers = append(r.dispatchers, d)
}

// classifierFor returns the first classifier that matches the envelope.
func (r *Registry) ClassifierFor(env *DataEnvelope) Classifier {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, c := range r.classifiers {
		if c.Match(env) {
			return c
		}
	}
	return nil
}

// processorsFor returns processors applicable to a data type, in
// registration order.
func (r *Registry) ProcessorsFor(dataType string) []Processor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Processor
	for _, p := range r.processors {
		if p.Applies(dataType) {
			out = append(out, p)
		}
	}
	return out
}

// evaluatorsFor returns evaluators applicable to a data type.
func (r *Registry) EvaluatorsFor(dataType string) []Evaluator {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Evaluator
	for _, e := range r.evaluators {
		if e.Applies(dataType) {
			out = append(out, e)
		}
	}
	return out
}

// dispatcherFor returns the dispatcher that handles a task type.
func (r *Registry) DispatcherFor(taskType string) Dispatcher {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, d := range r.dispatchers {
		if d.Handles(taskType) {
			return d
		}
	}
	return nil
}
