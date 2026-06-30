import { computed, inject, Injectable, signal } from '@angular/core';
import { environment } from '../../../environments/environment';
import { HttpClient } from '@angular/common/http';
import { Product } from '../../shared/models/product';
import { CartItem, Cart as CartEntity } from '../../shared/models/cart';
import { map } from 'rxjs/internal/operators/map';
import { DeliveryMethod } from '../../shared/models/deliveryMethod';

@Injectable({
  providedIn: 'root',
})
export class Cart {
  baseUrl = environment.apiUrl;
  private http = inject(HttpClient);
  cart = signal<CartEntity | null>(null);
  selectedDeliveryMethod = signal<DeliveryMethod | null>(null);
  itemCount = computed(() => this.cart()?.items.reduce((count, item) => count + item.quantity, 0) ?? 0);
  totals = computed(() => {
    const cart = this.cart();
    const deliveryMethod = this.selectedDeliveryMethod();
    if (!cart) return null;
    const subtotal = cart.items.reduce((sum, item) => sum + item.price * item.quantity, 0);
    const shipping = deliveryMethod ? deliveryMethod.price : 0;
    const discount = 0;
    return {
      subtotal, 
      shipping,
      discount,
      total: subtotal + shipping - discount
    }
  });

  getCart(id:string) {
    return this.http.get<CartEntity>(this.baseUrl + 'cart?id=' + id).pipe(
      map(cart => {
        this.cart.set(cart);
        return cart;
      })
    );
  }

  setCart(cart: CartEntity) {
    this.http.post<CartEntity>(this.baseUrl + 'cart', cart).subscribe({
      next: (cart) => this.cart.set(cart),
      error: (error) => console.error(error),
    });
  }

  addItemToCart(item: CartItem | Product, quantity = 1) {
    const cart = this.cart() ?? this.createCart();
    if (this.isProduct(item)) {
      item = this.mapProductItemToCartItem(item);
    }
    cart.items = this.addOrUpdateItem(cart.items, item, quantity);
    this.setCart(cart);
  }

  removeItemFromCart(productId: number, quantity = 1) {
    const cart = this.cart();
    if (!cart) return;
    const index = cart.items.findIndex(i => i.productId === productId);
    if (index !== -1) {
      if (cart.items[index].quantity > quantity) {
        cart.items[index].quantity -= quantity;
      } else {
        cart.items.splice(index, 1);
      }
      if (cart.items.length === 0) {
        this.deleteCart();
      } else {
        this.setCart(cart);
      }
    }
  }

  deleteCart() {
    this.http.delete(this.baseUrl + 'cart?id=' + this.cart()?.id).subscribe({
      next: () => {
        localStorage.removeItem('cart_id');
        this.cart.set(null);
      },
      error: (error) => console.error(error),
    });
  }

  private addOrUpdateItem(items: any, item: CartItem, quantity: number): any {
    const index = items.findIndex((i: CartItem) => i.productId === item.productId);
    if (index === -1) {
      item.quantity = quantity;
      items.push(item);
    } else {
      items[index].quantity += quantity;
    }
    return items;
  }

  private mapProductItemToCartItem(item: Product): CartItem {
    return {
      productId: item.id,
      productName: item.name,
      price: item.price,
      quantity: 0,
      pictureUrl: item.pictureUrl,
      artist: item.artist,
      genre: item.genre,
      label: item.label
    }
  }

  private isProduct(item: CartItem | Product): item is Product {
    return 'id' in item;
  }

  private createCart(): CartEntity {
    const cart = new CartEntity();
    localStorage.setItem('cart_id', cart.id);
    return cart;
  }
}
