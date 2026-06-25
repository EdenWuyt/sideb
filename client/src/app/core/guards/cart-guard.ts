import { inject } from '@angular/core/primitives/di';
import { CanActivateFn, Router } from '@angular/router';
import { Cart } from '../services/cart';
import { Snackbar } from '../services/snackbar';
import { of } from 'rxjs';

export const cartGuard: CanActivateFn = (route, state) => {
  const snackBar = inject(Snackbar);
  const cartService = inject(Cart);
  const router = inject(Router);

  if (cartService.cart() == null || cartService.cart()?.items.length === 0) {
    snackBar.error('Please add items to your cart before proceeding to checkout.'); 
    router.navigateByUrl('/cart');
    return of(false);
  }
  return of(true);
}
