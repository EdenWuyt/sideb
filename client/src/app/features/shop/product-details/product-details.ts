import { Component, inject, OnInit, signal } from '@angular/core';
import { Shop } from '../../../core/services/shop';
import { ActivatedRoute } from '@angular/router';
import { Product } from '../../../shared/models/product';
import { CurrencyPipe } from '@angular/common';
import { MatIcon } from '@angular/material/icon';
import { MatButton } from '@angular/material/button';
import { MatError, MatFormField, MatInput, MatLabel } from '@angular/material/input';
import { MatDivider } from "@angular/material/divider";
import { Cart } from '../../../core/services/cart';
import { FormsModule } from '@angular/forms';
import { Snackbar } from '../../../core/services/snackbar';
import { IntegerOnlyValidator } from '../../../shared/validators/integer-only-validators';

@Component({
  selector: 'app-product-details',
  imports: [
    CurrencyPipe,
    MatButton,
    MatIcon,
    MatFormField,
    MatInput,
    MatLabel,
    MatDivider,
    FormsModule,
    IntegerOnlyValidator
],
  templateUrl: './product-details.html',
  styleUrl: './product-details.css',
})
export class ProductDetails implements OnInit {
  private shopService = inject(Shop);
  private activatedRoute = inject(ActivatedRoute);
  private cartService = inject(Cart);
  protected readonly product = signal<Product | null>(null);
  quantityInCart = signal(0);
  quantity = signal(1);
  snackbar = inject(Snackbar);

  ngOnInit(): void {
    this.loadProduct();
  }

  loadProduct(): void {
    const id = this.activatedRoute.snapshot.paramMap.get('id');
    if (!id) return;
    this.shopService.getProduct(+id).subscribe({
      next: (product) => {
        this.product.set(product);
        this.updateQuantityInCart();
      },
      error: (error) => console.error(error),
    });
  }

  updateCart(): void {
    if (!this.product()) return;
    if (this.quantity() > this.quantityInCart()) {
      const itemsToAdd = this.quantity() - this.quantityInCart();
      this.quantityInCart.set(this.quantity());
      this.cartService.addItemToCart(this.product()!, itemsToAdd);
      this.snackbar.success(`Updated quantity to ${this.quantityInCart()} for this record`);
    } else if (this.quantity() < this.quantityInCart()) {
      const itemsToRemove = this.quantityInCart() - this.quantity();
      this.quantityInCart.set(this.quantity());
      this.cartService.removeItemFromCart(this.product()!.id, itemsToRemove);
      if (this.quantity() === 0) {
        this.quantity.set(1);
        this.snackbar.success(`Removed this record from your cart`);
      } else {
        this.snackbar.success(`Updated quantity to ${this.quantityInCart()} for this record`);
      }
    }
  }

  updateQuantityInCart(): void {
    this.quantityInCart.set(this.cartService.cart()?.items.find(i => i.productId === this.product()?.id)?.quantity ?? 0);
    if (this.quantityInCart() > 0) {
      this.quantity.set(this.quantityInCart());
    }
  }

  getButtonText(): string {
    return this.quantityInCart() > 0 ? 'Update Cart' : 'Add to Cart';
  }
}
