import { nanoid } from 'nanoid';

export type CartType = {
    id: string;
    items: CartItem[];
    deliveryMethodId?: number;
    paymentIntentId?: string;
    clientSecret?: string;
    coupon?: Coupon;
}

export type CartItem = {
    productId: number;
    productName: string;
    price: number;
    quantity: number;
    pictureUrl: string;
    artist: string;
    genre: string;
    label: string;
}

export type Coupon = {
    name: string;
    amountOff?: number;
    percentOff?: number;
    promotionCode: string;
    couponId: string;
}

export class Cart implements CartType {
    id: string = nanoid();
    items: CartItem[] = [];
    deliveryMethodId?: number;
    paymentIntentId?: string;
    clientSecret?: string;
    coupon?: Coupon;
}