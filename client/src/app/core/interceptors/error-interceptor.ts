import { HttpErrorResponse, HttpInterceptorFn } from '@angular/common/http';
import { inject } from '@angular/core/primitives/di';
import { NavigationExtras, Router } from '@angular/router';
import { catchError } from 'rxjs';
import { throwError } from 'rxjs/internal/observable/throwError';
import { Snackbar } from '../services/snackbar';

export const errorInterceptor: HttpInterceptorFn = (req, next) => {
  const router = inject(Router);
  const snackbar = inject(Snackbar);
  return next(req).pipe(  // pipe: used for the response of the HTTP request
    catchError((err: HttpErrorResponse) => {
      if (err) {
        switch (err.status) {
          case 400:
            if (err.error.errors) {
              const modelStateErrors = [];
              for (const key in err.error.errors) {
                if (err.error.errors[key]) {
                  modelStateErrors.push(err.error.errors[key]);
                }
              }
              throw modelStateErrors.flat();
            } else {
              snackbar.error(err.error.title || err.error);
            }
            break;
          case 401:
            snackbar.error(err.error.title || err.error);
            break;
          case 404:
            router.navigateByUrl('/not-found');
            break;
          case 500:
            const navigationExtras: NavigationExtras = { state: { error: err.error } };
            router.navigateByUrl('/server-error', navigationExtras);
            break;
          default:
            snackbar.error('Something unexpected went wrong.');
            break;
        }
      }
      return throwError(() => err); // pass the error to the caller of the HTTP request, so that it can be handled there if needed
    })
  );
};
