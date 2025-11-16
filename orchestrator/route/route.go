package route

import (
	"orchestrator/service"

	"github.com/go-chi/chi/v5"
)

type Router struct {
	Router chi.Router
}

func NewRouter(srv *service.Service) *Router {
	r := &Router{
		Router: chi.NewRouter(),
	}

	r.initRoutes(srv)

	return r
}

func (r *Router) initRoutes(srv *service.Service) {
	r.Router.Post("/start", srv.SeterStartOfScenario)
	r.Router.Get("/object", srv.GeterVideoObject)
	r.Router.Get("/meta", srv.GeterVideoMeta)
	r.Router.Get("/processes", srv.GeterCurrentProcesses)
}
